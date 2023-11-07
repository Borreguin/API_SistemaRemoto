import os.path

from starlette import status
from starlette.responses import FileResponse

from app.core.repositories import local_repositories
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
