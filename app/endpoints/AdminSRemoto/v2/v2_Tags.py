from fastapi import APIRouter
from starlette.responses import Response

from app.common.PI_connection.pi_util import search_pi_points_and_values
from app.core.v2CalculationEngine.util import get_pi_server


def v2_tags_endpoints(router: APIRouter):
    endpoint_uri = 'v2/tags'
    router.tags = ["v2-admin-sRemoto - TAGS"]

    search_tags_uri = endpoint_uri + '/search/{regex}'
    @router.get(search_tags_uri)
    def buscar_tags(regex: str, response: Response = Response()):
        """ Busca tags en el servidor PI """
        pi_server = get_pi_server()
        result = search_pi_points_and_values(pi_server, regex)
        success = len(result) > 0
        response.status_code = 200 if success else 404
        return result