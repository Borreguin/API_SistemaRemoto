from fastapi import APIRouter
from starlette.responses import Response

from app.db.constants import V2_SR_NODE_LABEL
from app.services.AdminSRemoto.AdminSRemotoService import *


def v2_node_id_endpoints(router: APIRouter):
    endpoint_uri = '/nodo/v2/id/{id}'
    router.tags = ["v2-admin-sRemoto - by id"]

    @router.delete(endpoint_uri)
    def v2_elimina_nodo_usando_ID_como_referencia(id: str, response: Response = Response()):
        resp, response.status_code = delete_elimina_nodo_usando_ID_como_referencia(id, version=V2_SR_NODE_LABEL)
        return resp

    @router.put(endpoint_uri)
    def v2_actualiza_cambios_menores_en_nodo(id: str, request_data: BasicNodeInfoRequest, response: Response = Response()):
        resp, response.status_code = put_actualiza_cambios_menores_en_nodo(id, request_data, version=V2_SR_NODE_LABEL)
        return resp

    @router.post(endpoint_uri)
    def v2_crea_nuevo_nodo_usando_ID(id, request_data: BasicNodeInfoRequest, response: Response = Response()):
        resp, response.status_code = post_crea_nuevo_nodo_usando_ID(id, request_data, version=V2_SR_NODE_LABEL)
        return resp
