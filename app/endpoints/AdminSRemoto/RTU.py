from fastapi import APIRouter
from starlette.responses import Response

from app.services.AdminSRemoto.v1.RTUService import *


def rtu_endpoints(router: APIRouter):
    endpoint_uri = '/rtu/{id_nodo}/{id_entidad}'
    router.tags = ["admin-sRemoto - RTU"]

    @router.get(endpoint_uri)
    def lista_RTU_de_entidad(id_nodo: str = "id nodo", id_entidad: str = "id entidad",
                             response: Response = Response()):
        """ Regresa la lista de RTU de una entidad \n
            Id nodo: id único del nodo \n
            Id entidad: id único de la entidad \n
            <b>404</b> Si el nodo o la entidad no existe
        """
        resp, response.status_code = get_lista_RTU_de_entidad(id_nodo, id_entidad)
        return resp

    @router.post(endpoint_uri)
    def ingresa_nueva_RTU_en_entidad(id_nodo: str = "id nodo", id_entidad: str = "id entidad",
                                     request_data: RTURequest = RTURequest(),
                                     response: Response = Response()):
        """
        Ingresa una nueva RTU en una entidad si esta no existe, caso contrario la edita \n
        Id nodo: id único del nodo \n
        Id entidad: id único de la entidad \n
        <b>404</b> Si el nodo o la entidad no existe \n
        """
        resp, response.status_code = post_ingresa_nueva_RTU_en_entidad(id_nodo, id_entidad, request_data)
        return resp

    @router.delete(endpoint_uri)
    def elimina_RTU_en_entidad(id_nodo: str = "id nodo", id_entidad: str = "id entidad",
                               request_data: RTURequestId = RTURequestId(), response: Response = Response()):
        """ Elimina una RTU en una entidad \n
            Id nodo: id único del nodo \n
            Id entidad: id único de la entidad \n
            <b>404</b> Si el nodo o la entidad no existe
        """

        resp, response.status_code = delete_elimina_RTU_en_entidad(id_nodo, id_entidad, request_data)
        return resp

    @router.get(endpoint_uri + '/{id_utr}')
    def obtiene_configuracion_RTU(id_nodo: str = "id nodo", id_entidad: str = "id entidad",
                                  id_utr: str = "id UTR", response: Response = Response()):
        """ Regresa la cofiguración de la RTU  \n
            Id nodo: id único del nodo  \n
            Id entidad: id único de la entidad  \n
            Id utr: id único de la entidad  \n
            <b>404</b> Si el nodo, la entidad o UTR no existe
        """
        resp, response.status_code = get_obtiene_configuracion_RTU(id_nodo, id_entidad, id_utr)
        return resp
