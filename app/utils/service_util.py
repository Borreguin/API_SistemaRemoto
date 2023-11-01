import datetime as dt
import os
from random import randint
from typing import Union

import pandas as pd
from fastapi import UploadFile

from flask_app.dto.mongo_engine_handler.sRNode import SRNodeFromDataFrames, SRNode


def is_excel_file(content_type: str) -> bool:
    return content_type in ['application/xls, application/vnd.ms-excel,  application/xlsx',
                            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']


async def get_node_from_excel_file(tipo: str, nombre: str, excel_file: UploadFile) \
        -> Union[tuple[None, str], tuple[SRNode, str]]:
    from app.core.repositories import local_repositories
    if not is_excel_file(excel_file.content_type):
        return None, "El formato del archivo no es aceptado"

    # path del archivo temporal a guardar para poderlo leer inmediatamente
    temp_file = os.path.join(local_repositories.TEMPORAL, f"{str(randint(0, 100))}_{excel_file.filename}")
    with open(temp_file, 'wb') as f:
        f.write(await excel_file.read())
    try:
        df_main = pd.read_excel(temp_file, sheet_name="main")
        df_tags = pd.read_excel(temp_file, sheet_name="tags")
    except Exception as e:
        return None, f'Not able to read the file, {e}'

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
        version = dt.datetime.now().strftime("@%Y-%b-%d_%Hh%M")
        destination = destination.replace(".xls", f"{version}.xls")

    with open(destination, 'wb') as f:
        f.write(stream_excel_file)
