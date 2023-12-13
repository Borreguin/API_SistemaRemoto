from __future__ import annotations

import uuid
from typing import List
import pandas as pd
import bcrypt
import os
import datetime as dt

from fastapi import UploadFile

from app import project_path
from app.utils.excel_util import is_excel_file


def get_hashed_text(plain_text: str) -> bytes:
    return bcrypt.hashpw(plain_text.encode('utf8'), bcrypt.gensalt())


def verify_hashed_text(plain_text: str, hashed_text: bytes) -> bool:
    return bcrypt.checkpw(plain_text.encode('utf8'), hashed_text)


def create_folder(path: str) -> str:
    to_create = os.path.join(project_path, path)
    if not os.path.exists(to_create):
        os.makedirs(to_create)
    exist = os.path.exists(to_create)
    return to_create if exist else None


def create_folders(path_list: List[str]) -> List[str]:
    result = list()
    for path in path_list:
        result.append(create_folder(path))
    return result


def to_dict(val):
    if hasattr(val, '__dict__'):
        return to_dict(val.__dict__)

    if isinstance(val, dict):
        resp = dict()
        for key, item in val.items():
            value = to_dict(item)
            resp[key] = value
        return resp

    if isinstance(val, list):
        return [to_dict(it) for it in val]

    return val


async def write_temporal_upload_file(upload_file: UploadFile, temp_path: str):
    filename = upload_file.filename
    # path del archivo temporal a guardar para poderlo leer inmediatamente
    temp_file = os.path.join(temp_path, f"{uuid.uuid4()}_{filename}")
    with open(temp_file, 'wb') as f:
        f.write(await upload_file.read())
    return os.path.exists(temp_file), temp_file


async def get_df_from_upload_file(upload_file: UploadFile, temp_path: str):
    if not is_excel_file(upload_file.content_type):
        return False, "Archivo Excel no compatible"

    try:
        success, temp_file = await write_temporal_upload_file(upload_file, temp_path)
        if not success:
            return False, "No fue posible subir este archivo"
        # obtener el dataframe:
        df = pd.read_excel(temp_file, engine='openpyxl')
        # una vez leído, eliminar archivo temporal
        os.remove(temp_file)
        return True, df, "Archivo leído correctamente"
    except Exception as e:
        msg = f"Error al leer información del archivo: {str(e)}"
        return False, pd.DataFrame(), msg

def get_time_in_minutes(ini_date: dt.datetime, end_date: dt.datetime):
    t_delta = end_date - ini_date
    time_in_minutes = t_delta.days * (60 * 24) + t_delta.seconds // 60 + (t_delta.seconds % 60) / 60
    return time_in_minutes