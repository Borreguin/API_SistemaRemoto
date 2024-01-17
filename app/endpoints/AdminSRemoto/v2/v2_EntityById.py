from fastapi import APIRouter
from starlette.responses import Response

from app.services.AdminSRemoto.v2.EntityService_v2 import get_entity_by_id


def v2_entity_id_endpoints(router: APIRouter):
    endpoint_uri = '/v2/entidad/id/{id}'
    router.tags = ["v2-admin-sRemoto - by id"]

    @router.get(endpoint_uri)
    def v2_obtiene_entidad_usando_ID_como_referencia(id:str, response: Response = Response()):
        """ Obtiene informacion de la entidad usando el ID como referencia """
        resp, response.status_code = get_entity_by_id(id)
        return resp
