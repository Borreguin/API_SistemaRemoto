import os
from typing import Tuple

import pandas as pd
from fastapi import UploadFile
from starlette import status
from starlette.responses import FileResponse

from app.core.repositories import local_repositories
from app.schemas.RequestSchemas import Option
from app.utils.excel_util import get_node_from_excel_file, save_excel_file_from_bytes
from app.db.v1.sRNode import SRNode, SRDataFramesFromDict


async def post_agrega_nodo_mediante_archivo_excel(tipo: str, nombre: str, excel_file: UploadFile) -> Tuple[dict, int]:
    """
    Permite añadir un nodo mediante un archivo excel
    Si el nodo ha sido ingresado correctamente, entonces el código es 200
    Si el nodo ya existe entonces error 409
    """
    # args = parsers.excel_upload.parse_args()
    nodo = SRNode.objects(nombre=nombre).first()
    if nodo is not None:
        return dict(success=False, msg=f"El nodo {[nombre]} ya existe"), status.HTTP_409_CONFLICT

    sr_node, msg = await get_node_from_excel_file(tipo, nombre, excel_file)
    if sr_node is None:
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST

    # for entity in node.entidades:
    #    for utr in entity.utrs:
    #        utr.create_consignments_container()
    sr_node.save()
    # Guardar como archivo Excel con versionamiento
    destination = os.path.join(local_repositories.S_REMOTO_EXCEL, excel_file.filename)
    save_excel_file_from_bytes(destination=destination, stream_excel_file=await excel_file.read())
    return dict(success=True, nodo=sr_node.to_summary(), msg=msg), status.HTTP_200_OK


async def put_actualizar_nodo_usando_excel(tipo: str, nombre: str, excel_file: UploadFile, option: Option):
    nodo = SRNode.objects(tipo=tipo, nombre=nombre).first()
    if nodo is None:
        return dict(success=False, msg=f"El nodo {[nombre]} no existe"), status.HTTP_400_BAD_REQUEST

    sr_node, msg = await get_node_from_excel_file(tipo, nombre, excel_file)
    if sr_node is None:
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST

    if option is None or option == option.EDIT:
        success, msg = nodo.add_or_replace_entities(sr_node.entidades)
        if not success:
            return dict(success=False, msg=str(msg)), status.HTTP_400_BAD_REQUEST
        nodo.save()
    elif option is not None and option == option.REEMPLAZAR:
        nodo.delete()
        sr_node.save()
        nodo = sr_node
    # crear contenedores de consignaciones si fuera necesario
    # for entity in nodo.entidades:
    #    for utr in entity.utrs:
    #        utr.create_consignments_container()
    nodo.save()
    # Guardar como archivo Excel con versionamiento
    destination = os.path.join(local_repositories.S_REMOTO_EXCEL, excel_file.filename)
    save_excel_file_from_bytes(destination=destination, stream_excel_file=await excel_file.read())
    return dict(success=True, nodo=nodo.to_summary(), msg=msg), status.HTTP_200_OK


async def get_descarga_excel_de_ultima_version_de_nodo(nombre: str, tipo: str):
    node = SRNode.objects(nombre=nombre, tipo=tipo).as_pymongo().exclude('id')
    if node.count() == 0:
        return dict(success=False, msg=f"No existe el nodo en la base de datos"), status.HTTP_404_NOT_FOUND
    node_as_dict = node[0]
    success, df_main, df_tags, msg = SRDataFramesFromDict(node_as_dict).convert_to_DataFrames()
    if not success:
        return dict(success=False, msg=msg), status.HTTP_409_CONFLICT
    file_name = f"{tipo}{nombre}.xlsx".replace("_", "@")
    path = os.path.join(local_repositories.TEMPORAL, file_name)
    with pd.ExcelWriter(path) as writer:
        df_main.to_excel(writer, sheet_name="main")
        df_tags.to_excel(writer, sheet_name="tags")
    return FileResponse(path=path, filename=file_name, media_type='application/octet-stream',
                        content_disposition_type="attachment"), status.HTTP_200_OK

