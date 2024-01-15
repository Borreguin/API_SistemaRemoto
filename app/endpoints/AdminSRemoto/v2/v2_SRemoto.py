from fastapi import APIRouter
from starlette.responses import Response

from app.services.AdminSRemoto.CommonV1AndV2.AdminSRemotoService import *


def v2_admin_sremoto_endpoints(router: APIRouter):
    endpoint_uri = "/v2/nodo"
    router.tags = ["v2-admin-sRemoto"]

    @router.put(endpoint_uri + '/id/{id_nodo}/activado', status_code=200)
    def v2_activa_nodo(id_nodo: str = "ID del nodo a cambiar", response: Response = Response()):
        """ Activa un nodo """
        resp, response.status_code = put_activa_desactiva_nodo(id_nodo, activado=True, version=V2_SR_NODE_LABEL)
        return resp

    @router.put(endpoint_uri + '/id/{id_nodo}/desactivado', status_code=200)
    def v2_desactiva_nodo(id_nodo: str = "ID del nodo a cambiar", response: Response = Response()):
        """ Desactiva un nodo """
        resp, response.status_code = put_activa_desactiva_nodo(id_nodo, activado=False, version=V2_SR_NODE_LABEL)
        return resp

    @router.get(endpoint_uri + '/{tipo}/{nombre}/{entidad_tipo}/{entidad_nombre}')
    def v2_retorna_entidades_de_nodo(tipo: str = "Tipo nodo", nombre: str = "Nombre nodo",
                                  entidad_tipo: str = "Entidad tipo",
                                  entidad_nombre: str = "Entidad nombre", response: Response = Response()):
        """ Retorna las entidades de un nodo """
        resp, response.status_code = get_retorna_entidades_de_nodo(tipo, nombre, entidad_tipo,
                                                                   entidad_nombre, version=V2_SR_NODE_LABEL)
        return resp

    @router.get(endpoint_uri + 's')
    @router.get(endpoint_uri + 's/')
    @router.get(endpoint_uri + 's/{filter_str}')
    def v2_muestra_todos_los_nombres_nodos_existentes(filter_str=None, response: Response = Response()):
        """
        Muestra todos los nombres de los nodos existentes si el filtro está vacio \n
        Los caracteres * son comodines de busqueda \n
        Ejemplo: ['pala', 'alambre', 'pétalo'] , \n
                *ala* => 'pala', 'alambre'  \n
                ala* => 'alambre'
        """
        resp, response.status_code = get_muestra_todos_los_nombres_nodos_existentes(filter_str, version=V2_SR_NODE_LABEL)
        return resp