from fastapi import APIRouter
from starlette.responses import Response

from app.services.AdminSRemoto.AdminSRemotoService import *


def v1_node_id_endpoints(router: APIRouter):
    endpoint_uri = '/nodo/id/{id}'
    router.tags = ["v1-admin-sRemoto - by id"]

    @router.delete(endpoint_uri)
    def v1_elimina_nodo_usando_ID_como_referencia(id: str, response: Response = Response()):
        resp, response.status_code = delete_elimina_nodo_usando_ID_como_referencia(id)
        return resp

    @router.put(endpoint_uri, deprecated=True)
    def v1_actualiza_cambios_menores_en_nodo(id: str, request_data: BasicNodeInfoRequest, response: Response = Response()):
        resp, response.status_code = put_actualiza_cambios_menores_en_nodo(id, request_data)
        return resp

    @router.post(endpoint_uri, deprecated=True)
    def v1_crea_nuevo_nodo_usando_ID(id, request_data: BasicNodeInfoRequest, response: Response = Response()):
        resp, response.status_code = post_crea_nuevo_nodo_usando_ID(id, request_data)
        return resp
