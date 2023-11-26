import os
import datetime as dt

from starlette import status
from starlette.responses import FileResponse

from app.core.repositories import local_repositories
from app.schemas.RequestSchemas import GroupedOption
from app.utils.utils import group_files


def get_obtiene_lista_archivos_en_repositorio(repo: str, agrupado: GroupedOption, filtrado=""):
    # Check /settings/config para añadir más repositorios
    path_repo = local_repositories.path_for(repo)
    if path_repo is None:
        return dict(success=False, msg="No existe el repositorio consultado"), status.HTTP_404_NOT_FOUND

    files = [f for f in os.listdir(path_repo) if os.path.isfile(os.path.join(path_repo, f))]
    result = [dict(name=file, datetime=str(dt.datetime.fromtimestamp(os.path.getmtime(os.path.join(path_repo, file)))))
              for file in files]
    if agrupado == GroupedOption.no_grouped:
        return dict(result=result), status.HTTP_200_OK

    # agrupar archivos por nombre y ordenar
    result = group_files(path_repo, files)
    if not len(filtrado):
        return dict(result=result), status.HTTP_200_OK
    filter_dict = dict()
    for k in result.keys():
        if filtrado.lower() in k.lower() or k.lower() in filtrado.lower():
            filter_dict[k] = result[k]
    return dict(result=filter_dict), status.HTTP_200_OK


def get_descarga_archivo_del_repositorio(repo="Nombre del repositorio", nombre="Nombre del archivo"):
    if repo not in local_repositories.repos:
        return dict(success=False, msg="No existe el repositorio consultado"), status.HTTP_404_NOT_FOUND
    path_repo = local_repositories.repo_path.get(repo)
    files = [f for f in os.listdir(path_repo) if os.path.isfile(os.path.join(path_repo, f))]
    files = [str(file).lower() for file in files]
    if nombre.lower() not in files:
        return dict(success=False, msg="No existe el archivo buscado")
    # resp = send_from_directory(repo, nombre, as_attachment=True)
    # return set_max_age_to_response(resp, 3)
    file_path = os.path.join(path_repo, nombre)
    return FileResponse(path=file_path, filename=nombre, media_type='application/octet-stream',
                        content_disposition_type="attachment")
