from fastapi import APIRouter
from starlette import status
from starlette.responses import Response
import datetime as dt

from app.db.constants import V2_SR_NODE_LABEL
from app.db.v2.entities.v2_sRNode import V2SRNode
from app.schemas.RequestSchemas import NodeNewName


def v2_node_type_and_name_endpoints(router: APIRouter):
    endpoint_uri = '/v2/nodo/{tipo}/{nombre}'
    router.tags = ["v2-admin-sRemoto - by Tipo and Nombre"]

    @router.get(endpoint_uri)
    def v2_busca_nodo_tipo_SRNode_en_base_de_datos(tipo: str = "Tipo de nodo",
                                                   nombre: str = "Nombre del nodo a buscar",
                                                   response: Response = Response()):
        nodo = V2SRNode.find(nombre=nombre, tipo=tipo)
        if nodo is None:
            response.status_code = status.HTTP_404_NOT_FOUND
            return nodo
        return nodo.to_dict()

    @router.put(endpoint_uri)
    def v2_actualiza_nombre_de_nodo(tipo: str = "Tipo de nodo", nombre: str = "Nombre del nodo a cambiar",
                                    request_data: NodeNewName = NodeNewName(), response: Response = Response()):

        nodo = V2SRNode.find(nombre=nombre, tipo=tipo)
        if nodo is None:
            response.status_code = status.HTTP_404_NOT_FOUND
            return nodo
        nodo.nombre, nodo.actualizado = request_data.nuevo_nombre, dt.datetime.now()
        nodo.save()
        return nodo.to_dict()
