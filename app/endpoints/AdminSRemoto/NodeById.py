from fastapi import APIRouter
from starlette.responses import Response

from app.services.AdminSRemoto.AdminSRemotoService import *


def node_id_endpoints(router: APIRouter):
    endpoint_uri = '/nodo/id/{id}'
    router.tags = ["admin-sRemoto - by id"]

    @router.delete(endpoint_uri)
    def elimina_nodo_usando_ID_como_referencia(id: str, response: Response = Response()):
        resp, response.status_code = delete_elimina_nodo_usando_ID_como_referencia(id)
        return resp

    @router.put(endpoint_uri)
    def actualiza_cambios_menores_en_nodo(id: str, request_data: BasicNodeInfoRequest, response: Response = Response()):
        resp, response.status_code = put_actualiza_cambios_menores_en_nodo(id, request_data)
        return resp

    @router.post(endpoint_uri)
    def crea_nuevo_nodo_usando_ID(id, request_data: BasicNodeInfoRequest, response: Response = Response()):
        resp, response.status_code = post_crea_nuevo_nodo_usando_ID(id, request_data)
        return resp
