from fastapi import APIRouter

from app.services.AdminSRemoto.v1.TagsService import *
from starlette.responses import Response


def tags_endpoints(router: APIRouter):
    endpoint_uri = '/tags/{id_nodo}/{id_entidad}/{id_utr}'
    router.tags = ["admin-sRemoto - TAGS"]

    @router.get(endpoint_uri)
    def obtiene_lista_tags(id_nodo: str = "id nodo", id_entidad: str = "id entidad", id_utr: str = "id utr",
                           response: Response = Response()):
        """ Regresa la lista de TAGS de una UTR  \n
            Id nodo: id único del nodo \n
            Id entidad: id único de la entidad \n
            Id utr: id único de la UTR \n
            <b>404</b> Si el nodo, entidad. no existe
        """
        resp, response.status_code = get_obtiene_lista_tags(id_nodo, id_entidad, id_utr)
        return resp

    @router.post(endpoint_uri)
    def agrega_lista_de_tags_en_UTR(id_nodo: str = "id nodo", id_entidad: str = "id entidad", id_utr: str = "id utr",
                                    request_data: TagListRequest = None, response: Response = Response()):
        """ Ingresa una lista de TAGS en una UTR si estas no existen, caso contrario las edita \n
            Id nodo: id único del nodo \n
            Id entidad: id único de la entidad \n
            Id UTR: id o código único de la UTR \n
            <b>404</b> Si el nodo o la entidad no existe
        """
        resp, response.status_code = post_agrega_lista_de_tags_en_UTR(id_nodo, id_entidad, id_utr, request_data)
        return resp

    @router.put(endpoint_uri)
    def edita_lista_de_tags_en_UTR(id_nodo: str = "id nodo", id_entidad: str = "id entidad", id_utr: str = "id utr",
                                   request_data: EditedListTagRequest = None, response: Response = Response()):
        """ Edita una lista de TAGS en una UTR basado en tag_name_original \n
            Id nodo: id único del nodo \n
            Id entidad: id único de la entidad \n
            Id UTR: id o código único de la UTR \n
            <b>404</b> Si el nodo, entidad o UTR no existe
        """
        resp, response.status_code = put_edita_lista_de_tags_en_UTR(id_nodo, id_entidad, id_utr, request_data)
        return resp

    @router.delete(endpoint_uri)
    def elimina_lista_de_tags_en_UTR(id_nodo: str = "id nodo", id_entidad: str = "id entidad", id_utr: str = "id utr",
                                     request_data: DeletedTagList = None, response: Response = Response()):
        """ Elimina una lista de tags basado en las ids de Nodo, Entidad, UTR \n
            Id nodo: id único del nodo \n
            Id entidad: id único de la entidad \n
            Id UTR: id o código único de la UTR \n
            <b>404</b> Si el nodo, entidad o UTR no existe
        """

        resp, response.status_code = delete_elimina_lista_de_tags_en_UTR(id_nodo, id_entidad, id_utr, request_data)
        return resp

    @router.put(endpoint_uri + '/from-excel')
    async def edita_lista_de_tags_en_UTR_usando_excel(id_nodo: str, id_entidad: str, id_utr: str,
                                                      file: UploadFile, response: Response = Response()):
        """ Edita una lista de TAGS en una UTR basado en tag_name_original de un archivo Excel \n
            Id nodo: id único del nodo \n
            Id entidad: id único de la entidad \n
            Id UTR: id o código único de la UTR \n
            <b>404</b> Si el nodo, entidad o UTR no existe
        """
        resp, response.status_code = await put_edita_lista_de_tags_en_UTR_usando_excel(id_nodo, id_entidad, id_utr, file)
        return resp
