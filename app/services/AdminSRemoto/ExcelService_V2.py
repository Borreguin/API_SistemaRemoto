import os.path

from starlette import status
from starlette.responses import FileResponse

from app.core.repositories import local_repositories
from app.db.v2.entities.v2_sRNode import V2SRNode
from app.schemas.RequestSchemas import Option
from app.utils.excel_util import *
from app.utils.migration_util import *


async def post_v2_genera_archivo_para_nueva_version(upload_file: UploadFile):

    success, temp_file = await write_temporal_file_from_upload_file(upload_file)
    if not success:
        return None, "No fue posible guardar el archivo temporal"

    success, df_main, df_tags, msg = v1_get_main_and_tags_from_excel_file(temp_file)
    if not success:
        return dict(success=success, msg=msg), status.HTTP_400_BAD_REQUEST
    df_tags_new = migration_process_df_tag(df_tags)
    df_main_new = migration_process_df_main(df_main)
    df_bahia_new = migration_process_df_bahia(df_tags_new)
    new_file_name = f'v2_{upload_file.filename}'
    file_path = os.path.join(local_repositories.TEMPORAL, new_file_name)
    success_temp_file = v2_write_excel_file_settings(df_main_new, df_bahia_new, df_tags_new, file_path)
    if success_temp_file:
        resp = FileResponse(path=file_path, filename=new_file_name, media_type='application/octet-stream',
                            content_disposition_type="attachment")
        return resp, status.HTTP_200_OK
    return dict(success=False, msg=f'No able to create file {new_file_name}'), status.HTTP_500_INTERNAL_SERVER_ERROR

async def v2_agrega_nodo_mediante_archivo_excel_service(tipo: str, nombre: str, upload_file: UploadFile,
                                                        replace=False, create_if_not_exists=False,
                                                        edit=False) -> Tuple[dict, int]:
    success, temp_file = await write_temporal_file_from_upload_file(upload_file)
    if not success:
        return dict(success=False, msg="No fue posible guardar el archivo temporal"), status.HTTP_500_INTERNAL_SERVER_ERROR

    success, df_main, df_bahia, df_tags, msg = v2_get_main_and_tags_from_excel_file(temp_file)
    if not success:
        return dict(success=success, msg=msg), status.HTTP_400_BAD_REQUEST

    if not replace:
        df_main, df_bahia, df_tags = filter_active_rows(df_main, df_bahia, df_tags)
        if len(df_main.index) == 0:
            return dict(success=False, msg="No hay nodos activos en el archivo"), status.HTTP_400_BAD_REQUEST

    success, msg, node = V2SRNode.find_or_create_if_not_exists_node(tipo, nombre, create_if_not_exists)
    will_continue = msg == 'Node created' or replace or edit
    if not success or not will_continue or node is None:
        msg = 'El nodo no puede ser reemplazado, ya existe' if msg == 'Node found' else msg
        msg = 'El nodo no existe' if node is None else msg
        return dict(success=False, msg=msg), status.HTTP_404_NOT_FOUND if node is None else status.HTTP_409_CONFLICT

    success, msg, new_node = node.create_or_edit_node_from_dataframes(df_main, df_bahia, df_tags, replace=replace, edit=edit)

    if not success:
        return dict(success=success, msg=msg), status.HTTP_400_BAD_REQUEST

    success, msg = new_node.save_safely()
    if not success:
        return dict(success=success, msg=msg), status.HTTP_400_BAD_REQUEST

    return dict(success=True, msg=msg, nodo=new_node.to_summary()), status.HTTP_200_OK

async def put_actualizar_nodo_usando_excel(tipo: str, nombre: str, upload_file: UploadFile,
                                           option: Option) -> Tuple[dict, int]:
    if option == option.REEMPLAZAR:
        return await v2_agrega_nodo_mediante_archivo_excel_service(tipo, nombre, upload_file,
                                                                   replace=True,
                                                                   create_if_not_exists=False)
    if option == option.EDIT:
        return await v2_agrega_nodo_mediante_archivo_excel_service(tipo, nombre, upload_file,
                                                                   replace=False,
                                                                   edit=True,
                                                                   create_if_not_exists=False)
    return dict(success=False, msg=f"Opción no válida"), status.HTTP_400_BAD_REQUEST