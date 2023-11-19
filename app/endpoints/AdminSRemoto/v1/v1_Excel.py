from fastapi import UploadFile, APIRouter
from starlette.responses import Response, FileResponse, StreamingResponse

from app.schemas.RequestSchemas import Option
from app.services.AdminSRemoto.ExcelService_V1 import post_agrega_nodo_mediante_archivo_excel, \
    put_actualizar_nodo_usando_excel, get_descarga_excel_de_ultima_version_de_nodo


# se puede consultar este servicio como: /url?nid=<cualquier_valor_random>
def v1_node_from_excel_endpoints(router: APIRouter):
    endpoint_uri = '/nodo/{tipo}/{nombre}/from-excel'
    router.tags = ["v1-admin-sRemoto - excel"]

    @router.post(endpoint_uri, deprecated=True)
    async def v1_agrega_nodo_mediante_archivo_excel(tipo: str, nombre: str, excel_file: UploadFile,
                                                 response: Response = Response()):
        resp, response.status_code = await post_agrega_nodo_mediante_archivo_excel(tipo, nombre, excel_file)
        return resp

    @router.put(endpoint_uri)
    async def actualizar_nodo_usando_excel(tipo: str, nombre: str, excel_file: UploadFile,
                                           option: Option = None,
                                           response: Response = Response()):
        """    Permite actualizar un nodo mediante un archivo excel \n
               Si el nodo no existe entonces error 404 \n
               EDIT: \n
               Si las entidades internas no existen entonces se añaden a la lista de entidades \n
               Las tags se actualizan conforme a lo especificado en el archivo \n
               REEMPLAZAR: \n
               El nodo completo es sustituido de acuerdo a lo especificado en el archivo
        """
        resp, response.status_code = await put_actualizar_nodo_usando_excel(tipo, nombre, excel_file, option)
        return resp

    @router.get(endpoint_uri)
    async def descarga_excel_de_ultima_version_de_nodo(nombre: str, tipo: str, response: Response = Response()):
        """ Descarga en formato excel la última versión del nodo """
        resp, response.status_code = await get_descarga_excel_de_ultima_version_de_nodo(nombre, tipo)
        return resp
