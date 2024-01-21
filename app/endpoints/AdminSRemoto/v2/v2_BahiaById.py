from fastapi import APIRouter
from starlette.responses import Response

from app.common.util import to_dict
from app.schemas.RequestSchemas import BahiaRequest
from app.services.AdminSRemoto.v2.BahiaService_v2 import get_bahia_by_id, create_bahia, update_bahia, delete_bahia


def v2_bahia_id_endpoints(router: APIRouter):
    endpoint_uri = '/v2/installation/{installation_id}/bahia/{document_id}'
    post_endpoint_uri = '/v2/installation/{installation_id}'
    router.tags = ["v2-admin-sRemoto - bahia"]

    @router.get(endpoint_uri)
    def v2_obtiene_bahia_usando_ID_como_referencia(installation_id: str, document_id: str,
                                                   response: Response = Response()):
        """ Obtiene informacion de la bahia usando el ID como referencia """
        resp, response.status_code = get_bahia_by_id(installation_id, document_id)
        return resp

    @router.post(post_endpoint_uri)
    def v2_crea_bahia_usando_ID_como_referencia(installation_id: str, bahia: BahiaRequest,
                                                response: Response = Response()):
        """ Crea una bahia usando el ID como referencia """
        resp, response.status_code = create_bahia(installation_id, to_dict(bahia))
        return resp

    @router.put(endpoint_uri)
    def v2_actualiza_bahia_usando_ID_como_referencia(installation_id: str, document_id: str, bahia: BahiaRequest,
                                                     response: Response = Response()):
        """ Actualiza una bahia usando el ID como referencia """
        resp, response.status_code = update_bahia(installation_id, document_id, to_dict(bahia))
        return resp

    @router.delete(endpoint_uri)
    def v2_elimina_bahia_usando_ID_como_referencia(installation_id: str, document_id: str,
                                                   response: Response = Response()):
        """ Elimina una bahia usando el ID como referencia """
        resp, response.status_code = delete_bahia(installation_id, document_id)
        return resp
