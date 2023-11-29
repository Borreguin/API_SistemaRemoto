from fastapi import APIRouter
from starlette import status
from starlette.responses import Response
import datetime as dt

from app.db.constants import V1_SR_NODE_LABEL
from app.db.db_util import find_node_by_name_and_type
from app.schemas.RequestSchemas import NodeNewName


def v1_node_type_and_name_endpoints(router: APIRouter):
    endpoint_uri = '/nodo/{tipo}/{nombre}'
    router.tags = ["v1-admin-sRemoto - by Tipo and Nombre"]

    @router.get(endpoint_uri)
    def v1_busca_nodo_tipo_SRNode_en_base_de_datos(tipo: str = "Tipo de nodo",
                                                nombre: str = "Nombre del nodo a buscar",
                                                response: Response = Response()):
        nodo = find_node_by_name_and_type(tipo, nombre, version=V1_SR_NODE_LABEL)
        if nodo is None:
            response.status_code = status.HTTP_404_NOT_FOUND
            return nodo
        return nodo.to_dict()

    @router.put(endpoint_uri)
    def v1_actualiza_nombre_de_nodo(tipo: str = "Tipo de nodo", nombre: str = "Nombre del nodo a cambiar",
                                 request_data: NodeNewName = NodeNewName(), response: Response = Response()):

        nodo = find_node_by_name_and_type(tipo, nombre, version=V1_SR_NODE_LABEL)
        if nodo is None:
            response.status_code = status.HTTP_404_NOT_FOUND
            return nodo
        nodo.nombre, nodo.actualizado = request_data.nuevo_nombre, dt.datetime.now()
        nodo.save()
        return nodo.to_dict()
