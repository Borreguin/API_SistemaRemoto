from fastapi import UploadFile, APIRouter
from starlette.responses import Response, FileResponse, StreamingResponse

from app.schemas.RequestSchemas import Option
from app.services.AdminSRemoto.ExcelService_V1 import post_agrega_nodo_mediante_archivo_excel, \
    put_actualizar_nodo_usando_excel, get_descarga_excel_de_ultima_version_de_nodo
from app.services.AdminSRemoto.ExcelService_V2 import post_v2_genera_archivo_para_nueva_version


# se puede consultar este servicio como: /url?nid=<cualquier_valor_random>
def v2_node_from_excel_endpoints(router: APIRouter):
    router.tags = ["v2-admin-sRemoto - excel"]

    endpoint_update = '/v2/upgrade/from-excel'

    @router.post(endpoint_update)
    async def v2_genera_archivo_para_nueva_version_desde_version_1(excel_file: UploadFile, response: Response = Response()):
        """ Genera un archivo Excel con el nuevo formato para la actualizacion del sistema """
        resp, response.status_code = await post_v2_genera_archivo_para_nueva_version(excel_file)
        return resp

    # endpoint_tipo_nombre_uri = '/v2/nodo/{tipo}/{nombre}/from-excel'
    # @router.post(endpoint_tipo_nombre_uri)
    # async def v2_agrega_nodo_mediante_archivo_excel(tipo: str, nombre: str, excel_file: UploadFile,
    #                                              response: Response = Response()):
    #     resp, response.status_code = await post_agrega_nodo_mediante_archivo_excel(tipo, nombre, excel_file)
    #     return resp
    #
    # @router.put(endpoint_tipo_nombre_uri)
    # async def v2_actualizar_nodo_usando_excel(tipo: str, nombre: str, excel_file: UploadFile,
    #                                        option: Option = None,
    #                                        response: Response = Response()):
    #     """    Permite actualizar un nodo mediante un archivo excel \n
    #            Si el nodo no existe entonces error 404 \n
    #            EDIT: \n
    #            Si las entidades internas no existen entonces se añaden a la lista de entidades \n
    #            Las tags se actualizan conforme a lo especificado en el archivo \n
    #            REEMPLAZAR: \n
    #            El nodo completo es sustituido de acuerdo a lo especificado en el archivo
    #     """
    #     resp, response.status_code = await put_actualizar_nodo_usando_excel(tipo, nombre, excel_file, option)
    #     return resp
    #
    # @router.get(endpoint_tipo_nombre_uri, response_class=FileResponse)
    # async def v2_descarga_excel_de_ultima_version_de_nodo(nombre: str, tipo: str, response: Response = Response()):
    #     """ Descarga en formato excel la última versión del nodo """
    #     headers = {
    #         'Content-Disposition': 'attachment; filename="filename.xlsx"'
    #     }
    #     resp = get_descarga_excel_de_ultima_version_de_nodo(nombre, tipo)
    #     #  filename=os.path.dirname(path)
    #
    #     return resp
