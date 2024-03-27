from __future__ import annotations
from fastapi import APIRouter
from starlette.responses import Response

from app.schemas.RequestSchemas import TagListRequest, RegexRequest
from app.services.AdminSRemoto.v2.TagService_v2 import get_values_for_tags_using_regex, get_values_for_tags


def v2_tags_endpoints(router: APIRouter):
    endpoint_uri = '/v2/tags'
    router.tags = ["v2-admin-sRemoto - TAGS"]

    search_tags_uri = endpoint_uri + '/search/{filter}'

    @router.get(search_tags_uri)
    def buscar_tags(filter: str):
        return buscar_tags_con_regex(filter, None)

    search_tags_with_regex_uri = endpoint_uri + '/search/{filter}'

    @router.post(search_tags_with_regex_uri)
    def buscar_tags_con_regex(filter: str, request_data: RegexRequest = None, response: Response = Response()):
        if filter == '*' or filter == ' ':
            response.status_code = 404
            return dict(success=False, tags=[], msg="No filter applied")
        """ Busca tags en el servidor PI """
        if request_data is not None:
            regex = request_data.regex
        else:
            regex = None
        tags_values = get_values_for_tags_using_regex(filter, regex)
        success = len(tags_values) > 0
        response.status_code = 200 if success else 404
        return dict(success=success, tags=tags_values, msg="Not found" if not success else "Values were found")

    get_tag_values_uri = endpoint_uri + '/values'

    @router.post(get_tag_values_uri)
    def obtener_valores_de_tags(request_data: TagListRequest, response: Response = Response()):
        """ Obtiene valores de tags en el servidor PI """
        tags_values = get_values_for_tags(request_data)
        success = len(tags_values) > 0
        response.status_code = 200 if success else 404
        return dict(success=success, tags=tags_values, msg="Not found" if not success else "Values were found")