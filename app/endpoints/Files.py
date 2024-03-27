from fastapi import APIRouter
from starlette.responses import Response, FileResponse

from app.core.config import Settings
from app.schemas.RequestSchemas import GroupedOption
from app.services.FilesService import get_obtiene_lista_archivos_en_repositorio, get_descarga_archivo_del_repositorio

router = APIRouter(
    prefix=f"{Settings.API_PREFIX}/files",
    tags=["files"],
    responses={404: {"description": "Not found"}},
)

repo_uri = '/{repo}'


@router.get(repo_uri)
@router.get(repo_uri + '/{agrupado}')
@router.get(repo_uri + '/{agrupado}/')
@router.get(repo_uri + '/{agrupado}/{filtrado}')
def obtiene_lista_archivos_en_repositorio(repo="Nombre del repositorio",
                                          agrupado: GroupedOption = GroupedOption.no_grouped, filtrado="",
                                          response: Response = Response()):
    """ Trae una lista de los archivos disponibles en el repositorio \n
            nombre: nombre del repositorio \n
            agrupado: "agrupado", "no-agrupado" \n
            fitrado: archivos a filtrar \n
            Repositorios disponibles: [s_remoto_excel, s_central_excel, output] \n
    """
    resp, response.status_code = get_obtiene_lista_archivos_en_repositorio(repo, agrupado, filtrado)
    return resp


file_uri = '/-/file/{repo}/{nombre}'


@router.get(file_uri, response_class=FileResponse)
def descarga_archivo_del_repositorio(repo="Nombre del repositorio", nombre="Nombre del archivo",
                                     response: Response = Response()):
    """
        Descarga un archivo de un repositorio \n
        repo: Nombre del repositorio [s_remoto_excel, s_central_excel] \n
        nombre: Nombre del archivo \n
        se puede consultar este servicio como: /url?nid=<cualquier_valor_random> \n
    """
    resp, response.status_code = get_descarga_archivo_del_repositorio(repo, nombre)
    return resp
