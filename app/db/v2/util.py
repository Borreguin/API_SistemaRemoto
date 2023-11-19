import re
import traceback
from enum import Enum
from typing import List

import pandas as pd

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

def get_or_replace_entities_and_installations_from_dataframe(df_main: pd.DataFrame,
                                                             replace=False,
                                                             edit=False) -> tuple[bool, str, list[V2SREntity]]:
    entities = get_entities_from_dataframe(df_main)
    msg = f"Se han agregado las entidades y sus instalaciones"
    check_msg = str()
    total_success = True
    for entidad in entities:
        df_entidad = df_main[(df_main[cl_entidad_tipo] == entidad.entidad_tipo) &
                             (df_main[cl_entidad_nombre] == entidad.entidad_nombre)]
        # bring the current installations
        fetched_installations = [instalacion.fetch().instalacion_ems_code for instalacion in entidad.instalaciones] if entidad.instalaciones is not None else []
        for ix in df_entidad.index:
            instalacion_values = dict(df_entidad[v2_instalacion_properties].loc[ix])
            success, _msg, instalacion = create_or_replace_installation(instalacion_values, replace=replace, edit=edit)
            already_exists, check = False, None
            if len(fetched_installations) > 0:
                check = [i for i, ems_code in enumerate(fetched_installations)
                         if ems_code == instalacion.instalacion_ems_code]
                already_exists = len(check) > 0

            if not success:
                check_msg += f"\n{_msg}"
                total_success = False
                continue

            if already_exists:
                entidad.instalaciones[check[0]] = instalacion
                continue

            entidad.instalaciones.append(instalacion)

    return total_success, msg + check_msg if not total_success else msg, entities

def create_or_replace_installation(instalacion_values: dict, replace=False, edit=False) -> tuple[bool, str, V2SRInstallation]:
    from app.db.util import create_instalation
    instalacion = V2SRInstallation.find_by_ems_code(instalacion_values[cl_instalacion_ems_code])
    # if exists and not replace
    replace = replace or edit
    if isinstance(instalacion, V2SRInstallation) and not replace:
        return True, f"La instalación ya existe", instalacion
    # create
    if instalacion is None:
        return create_instalation(instalacion_values)
    # replace or edit
    for att in v2_instalacion_properties:
        setattr(instalacion, att, instalacion_values[att])
    return True, f"La instalación fue reemplazada", instalacion

def get_or_replace_bahias_and_tags_from_dataframe(instalacion:V2SRInstallation,
                                                  df_bahias: pd.DataFrame,
                                                  df_tags: pd.DataFrame,
                                                  replace=False,
                                                  edit=False) -> tuple[bool, bool, str, V2SRInstallation]:
    try:
        common_columns = [cl_instalacion_ems_code, cl_bahia_code, cl_nivel_voltaje]
        df_merged = pd.merge(df_bahias, df_tags, on=common_columns, how='inner', suffixes=('_bahia', '_tag'))
        df_merged = df_merged[df_merged[cl_instalacion_ems_code] == instalacion.instalacion_ems_code]
        total_msg, total_success = '', True
        if df_merged.empty:
            return False, False, f"El DataFrame se encuentra vacío para {instalacion.instalacion_ems_code}", instalacion

        for group_values, df_group in df_merged.groupby(by=common_columns + [cl_bahia_nombre, cl_activado_bahia]):
            instalacion_ems_code, bahia_code, voltaje, bahia_nombre, activado_bahia = group_values
            bahia = V2SRBahia(bahia_code, bahia_nombre, voltaje, activado=activado_bahia != '')
            success_instalacion, msg, instalacion = replace_or_edit_bahia(instalacion, bahia, df_group, replace=replace, edit=edit)
            total_success = total_success and success_instalacion
            if success_instalacion:
                instalacion.save_safely()
            else:
                total_msg += f"\n{msg}"
                error_log.error(msg)
        return True, total_success, total_msg, instalacion
    except Exception as e:
        msg = f"No es posible generar desde el archivo excel: {e}"
        tb = traceback.format_exc()
        error_log.error(msg)
        error_log.error(tb)
        return False, False, msg, instalacion

def replace_or_edit_bahia(instalacion: V2SRInstallation,
                          bahia: V2SRBahia,
                          df_group: pd.DataFrame,
                          replace=False,
                          edit=False) -> tuple[bool, str, V2SRInstallation]:
    found_idx = None
    if instalacion.bahias is not None:
        check = [i for i,b in enumerate(instalacion.bahias)
                       if b.bahia_code == bahia.bahia_code and b.voltaje == bahia.voltaje]
        found_idx = check[0] if len(check) > 0 else None

    new_tags = replace_or_edit_tags(df_group, bahia.tags, replace=replace, edit=edit)
    bahia.tags = new_tags

    if replace or edit and found_idx is not None:
        instalacion.bahias[found_idx] = bahia
    elif found_idx is None:
        instalacion.bahias.append(bahia)

    return True, f"Se han agregado las bahias y tags al nodo", instalacion

def replace_or_edit_tags(df_group: pd.DataFrame, old_tags: List[V2SRTag], replace=False, edit=False) -> List[V2SRTag]:
    old_tags = old_tags if old_tags is not None else []

    new_tags = []
    if not df_group.empty:
        df_group.drop_duplicates(subset=cl_tag_name, inplace=True)
        for ix in df_group.index:
            tag_values = dict(df_group[v2_tag_dict_values].loc[ix])
            activado_tag = tag_values.pop(cl_activado_tag, 'x')
            tag_values[cl_activado] = activado_tag != ''
            tag = V2SRTag(**tag_values)
            new_tags.append(tag)

    is_new_tags = len(old_tags) == 0 and len(new_tags) > 0
    if replace or is_new_tags:
        return new_tags

    if edit:
        for tag in new_tags:
            check = [i for i,t in enumerate(old_tags) if t.tag_name == tag.tag_name]
            if len(check) > 0:
                # editing old tag
                old_tags[check[0]] = tag
            else:
                # adding new tag
                old_tags.append(tag)
    return old_tags