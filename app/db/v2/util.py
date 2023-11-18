import re
import traceback
from enum import Enum

import pandas as pd
from mongoengine import Document, NotUniqueError

from app.common import error_log
from app.db.v2.entities.v2_sRBahia import V2SRBahia
from app.db.v2.entities.v2_sREntity import V2SREntity
from app.db.v2.entities.v2_sRInstallation import V2SRInstallation
from app.db.v2.entities.v2_sRTag import V2SRTag
from app.utils.excel_constants import *


class MongoDBErrorEnum(str, Enum):
    DUPLICATED = "Duplicated key"


def tryExcept(func):
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)
            return True
        except Exception as e:
            print(f">>>>>\tNot able to execute due to: \n{e}")
            return False

    return wrapper


def save_mongo_document_safely(document: Document):
    try:
        document.save()
        return True, 'Document saved successfully'
    except NotUniqueError as e:
        dup_key, collection = find_collection_and_dup_key(f'{e}')
        return False, f"No unique key for: {dup_key} in {collection}"
    except Exception as e:
        return False, f"No able to save: {e}"


def find_collection_and_dup_key(exception: str):
    regex_dup_key = re.compile('dup key:(\\s*\\{[\\w|\\s|\\.|\\:|\\"]*\\})')
    regex_collection = re.compile('(collection:\\s*[\\w|\\.]+)')
    dup_key = regex_dup_key.findall(exception)
    collection = regex_collection.findall(exception)
    return dup_key[0] if len(dup_key) > 0 else '', collection[0] if len(collection) > 0 else ''


def get_entities_from_dataframe(df_main: pd.DataFrame) -> list[V2SREntity]:
    entities = []
    for (entidad_tipo, entidad_nombre, activado), df_group in df_main.groupby(by=v2_entidad_properties):
        entidad = V2SREntity(entidad_tipo, entidad_nombre, activado=activado)
        entities.append(entidad)
    return entities

def get_entities_and_installations_from_dataframe(df_main: pd.DataFrame) -> list[V2SREntity]:
    entities = get_entities_from_dataframe(df_main)
    for entidad in entities:
        df_entidad = df_main[(df_main[cl_entidad_tipo] == entidad.entidad_tipo) &
                             (df_main[cl_entidad_nombre] == entidad.entidad_nombre)]
        for ix in df_entidad.index:
            instalacion_values = dict(df_entidad[v2_instalacion_properties].loc[ix])
            instalacion = V2SRInstallation.find_by_ems_code(instalacion_values[cl_instalacion_ems_code])
            # if exists, add to entity
            if isinstance(instalacion, V2SRInstallation):
                entidad.instalaciones.append(instalacion)
                continue
            # create and add to entity
            if instalacion is None:
                instalacion = V2SRInstallation(**instalacion_values)
                success, msg = instalacion.save_safely()
                if success:
                    entidad.instalaciones.append(instalacion)
    return entities

def get_bahias_and_tags_from_dataframe(instalacion:V2SRInstallation,
                                       df_bahias: pd.DataFrame,
                                       df_tags: pd.DataFrame) -> tuple[bool, str, V2SRInstallation]:
    try:
        common_columns = [cl_instalacion_ems_code, cl_bahia_code, cl_nivel_voltaje]
        df_merged = pd.merge(df_bahias, df_tags, on=common_columns, how='inner', suffixes=('_bahia', '_tag'))
        df_merged = df_merged[df_merged[cl_instalacion_ems_code] == instalacion.instalacion_ems_code]
        if df_merged.empty:
            return False, f"El DataFrame se encuentra vacío", instalacion

        for group_values, df_group in df_merged.groupby(by=common_columns + [cl_bahia_nombre, cl_activado_bahia]):
            instalacion_ems_code, bahia_code, voltaje, bahia_nombre, activado_bahia = group_values
            bahia = V2SRBahia(bahia_code, bahia_nombre, voltaje, activado=activado_bahia != '')
            instalacion.bahias.append(bahia)
            for ix in df_group.index:
                tag_values = dict(df_group[v2_tag_dict_values].loc[ix])
                tag_values[cl_activado] = tag_values[cl_activado_tag] != ''
                tag_values.pop(cl_activado_tag)
                tag = V2SRTag(**tag_values)
                bahia.tags.append(tag)
        return True, f"Se han agregado las bahias y tags al nodo", instalacion
    except Exception as e:
        msg = f"No es posible generar desde el archivo excel: {e}"
        tb = traceback.format_exc()
        error_log.error(msg)
        error_log.error(tb)
        return False, msg, instalacion
