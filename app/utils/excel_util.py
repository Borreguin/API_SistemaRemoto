from __future__ import annotations
import datetime as dt
import os
from random import randint
from typing import Union, List, Tuple, Any

import pandas as pd
from fastapi import UploadFile
from pandas import DataFrame

from app.common import error_log
from app.utils.excel_constants import *
from app.db.v1.sRNode import SRNodeFromDataFrames, SRNode


def is_excel_file(content_type: str) -> bool:
    return content_type in ['application/xls, application/vnd.ms-excel,  application/xlsx',
                            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']


async def write_temporal_file_from_upload_file(upload_file: UploadFile):
    from app.core.repositories import local_repositories
    temp_file = os.path.join(local_repositories.TEMPORAL, f"{str(randint(0, 100))}_{upload_file.filename}")
    with open(temp_file, 'wb') as f:
        f.write(await upload_file.read())
    return os.path.exists(temp_file), temp_file


def v1_get_main_and_tags_from_excel_file(file_path: str) -> tuple[bool, DataFrame, DataFrame, str]:
    try:
        df_main = pd.read_excel(file_path, sheet_name="main", engine='openpyxl')
        missing_columns = get_missing_columns(df_main, v1_main_sheet_columns)
        if len(missing_columns) > 0:
            return (False, pd.DataFrame(), pd.DataFrame(),
                    f"Hoja main: Las siguientes columnas estan faltando: {missing_columns}")
        df_tags = pd.read_excel(file_path, sheet_name="tags", engine='openpyxl')
        df_tags.drop_duplicates(subset=[cl_tag_name], inplace=True)
        missing_columns = get_missing_columns(df_tags, v1_tags_sheet_columns)
        if len(missing_columns) > 0:
            return (False, pd.DataFrame(), pd.DataFrame(),
                    f"Hoja tags: Las siguientes columnas estan faltando: {missing_columns}")
        return True, df_main, df_tags, "File was read correctly"
    except Exception as e:
        return False, pd.DataFrame(), pd.DataFrame(), f'Not able to read the file, {e}'

def unify_nivel_voltaje_for_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df[column] = df[column].apply(lambda x: unify_nivel_voltaje(x))
    return df

def unify_nivel_voltaje(x) -> str:
    try:
        value = float(x)
        value = round(value, 2)
    except ValueError:
        if ',' in x:
            value = x.replace(",", ".")
            return unify_nivel_voltaje(value)
        value = None
    return str(value)

def v2_get_main_and_tags_from_excel_file(file_path: str) -> tuple[bool, DataFrame, DataFrame, DataFrame, str]:
    try:
        df_main = pd.read_excel(file_path, sheet_name="main", engine='openpyxl')
        missing_columns = get_missing_columns(df_main, v2_main_sheet_columns)
        if len(missing_columns) > 0:
            return (False, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
                    f"Hoja main: Las siguientes columnas estan faltando: {missing_columns}")
        df_bahia = pd.read_excel(file_path, sheet_name="bahias", engine='openpyxl')
        df_bahia = df_bahia.fillna('')
        df_bahia = unify_nivel_voltaje_for_column(df_bahia, cl_nivel_voltaje)
        missing_columns = get_missing_columns(df_bahia, v2_bahias_sheet_columns)
        if len(missing_columns) > 0:
            return (False, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
                    f"Hoja bahia: Las siguientes columnas estan faltando: {missing_columns}")
        df_tags = pd.read_excel(file_path, sheet_name="tags", engine='openpyxl')
        df_tags = df_tags.fillna('')
        df_tags = unify_nivel_voltaje_for_column(df_tags, cl_nivel_voltaje)
        missing_columns = get_missing_columns(df_tags, v2_tags_sheet_columns)
        df_tags.drop_duplicates(subset=[cl_tag_name], inplace=True)
        if len(missing_columns) > 0:
            return (False, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
                    f"Hoja tags: Las siguientes columnas estan faltando: {missing_columns}")
        return True, df_main, df_bahia, df_tags, "File was read correctly"
    except Exception as e:
        return False, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), f'Not able to read the file, {e}'

def filter_active_rows(df_main: pd.DataFrame, df_bahia: pd.DataFrame, df_tags: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_bahia = df_bahia[df_bahia[cl_activado] != '']
    df_tags = df_tags[df_tags[cl_activado] != '']
    df_main = df_main[df_main[cl_activado] != '']
    return df_main, df_bahia, df_tags

async def get_node_from_excel_file(tipo: str, nombre: str, excel_file: UploadFile) \
        -> Union[tuple[None, str], tuple[SRNode, str]]:
    if not is_excel_file(excel_file.content_type):
        return None, "El formato del archivo no es aceptado"

    # path del archivo temporal a guardar para poderlo leer inmediatamente
    success, temp_file = write_temporal_file_from_upload_file(excel_file)
    if not success:
        return None, "No fue posible guardar el archivo temporal"

    success, df_main, df_tags, msg = v1_get_main_and_tags_from_excel_file(temp_file)
    if not success:
        return None, msg

    # una vez leído, eliminar archivo temporal
    os.remove(temp_file)

    # create a virtual node:
    v_node = SRNodeFromDataFrames(tipo, nombre, df_main, df_tags)
    success, msg = v_node.validate()
    if not success:
        # el archivo no contiene el formato correcto
        return None, msg
    # create a final node to save if is successful
    success, node = v_node.create_node()
    if not success:
        return None, str(node)
    node.actualizado = dt.datetime.now()
    return node, 'El nodo ha sido actualizado exitosamente'


def save_excel_file_from_bytes(destination, stream_excel_file):
    try:
        n = 7
        last_file = destination.replace(".xls", f"@{n}.xls")
        first_file = destination.replace(".xls", "@1.xls")
        for i in range(n, 0, -1):
            file_n = destination.replace(f".xls", f"@{str(i)}.xls")
            file_n_1 = destination.replace(f".xls", f"@{str(i + 1)}.xls")
            if os.path.exists(file_n):
                os.rename(file_n, file_n_1)
        if os.path.exists(last_file):
            os.remove(last_file)
        if not os.path.exists(first_file) and os.path.exists(destination):
            os.rename(destination, first_file)

    except Exception as e:
        error_log.info(f"save_excel_file_from_bytes error: {e}")
        version = dt.datetime.now().strftime("@%Y-%b-%d_%Hh%M")
        destination = destination.replace(".xls", f"{version}.xls")

    with open(destination, 'wb') as f:
        f.write(stream_excel_file)


def all_columns_in_dataframe(df: pd.DataFrame, columns: List[str]) -> bool:
    return all([c in list(df.columns) for c in columns])


def get_missing_columns(df: pd.DataFrame, columns: List[str]) -> List[str]:
    return [c for c in columns if not c in list(df.columns)]

def convert_to_float(value: str | float) -> float:
    try:
        return float(value)
    except ValueError:
        if isinstance(value, str) and ',' in value:
            value = value.replace(",", ".")
            return convert_to_float(value)
        return 0