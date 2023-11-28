from fastapi import APIRouter
from starlette.responses import Response

from app.schemas.RequestSchemas import InstallationRequest
from app.services.AdminSRemoto.v2.EntityService_v2 import get_entity_by_id
from app.services.AdminSRemoto.v2.InstallationService_v2 import get_installation_by_id, post_installation


def v2_installation_id_endpoints(router: APIRouter):
    endpoint_uri = '/v2/instalacion/id/{id}'
    router.tags = ["v2-admin-sRemoto - by id"]

    @router.get(endpoint_uri)
    def v2_obtiene_instalacion_usando_ID_como_referencia(id:str, response: Response = Response()):
        """ Obtiene informacion de la instalacion usando el ID como referencia """
        resp, response.status_code = get_installation_by_id(id)
        return resp


    @router.post('/v2/instalacion/entidad-id/{entidad_id}')
    def v2_obtiene_instalacion_usando_ID_como_referencia(entidad_id:str, request_data: InstallationRequest,
                                                         response: Response = Response()):
        """ Crear instalacion usando el ID como referencia """
        resp, response.status_code = post_installation(entidad_id, request_data)
        return resp