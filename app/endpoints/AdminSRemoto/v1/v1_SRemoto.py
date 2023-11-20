from fastapi import APIRouter
from starlette.responses import Response

from app.db.constants import V1_SR_NODE_LABEL
from app.services.AdminSRemoto.AdminSRemotoService import put_activa_desactiva_nodo, get_retorna_entidades_de_nodo, \
    get_muestra_todos_los_nombres_nodos_existentes


def v1_admin_sremoto_endpoints(router: APIRouter):
    router.tags = ["v1-admin-sRemoto"]

    @router.put('/nodo/id/{id_nodo}/activado', status_code=200, deprecated=True)
    def v1_activa_nodo(id_nodo: str = "ID del nodo a cambiar", response: Response = Response()):
        resp, response.status_code = put_activa_desactiva_nodo(id_nodo, activado=True, version=V1_SR_NODE_LABEL)
        return resp

    @router.put('/nodo/id/{id_nodo}/desactivado', status_code=200, deprecated=True)
    def v1_desactiva_nodo(id_nodo: str = "ID del nodo a cambiar", response: Response = Response()):
        resp, response.status_code = put_activa_desactiva_nodo(id_nodo, activado=False, version=V1_SR_NODE_LABEL)
        return resp

    @router.get('/nodo/{tipo}/{nombre}/{entidad_tipo}/{entidad_nombre}', deprecated=True)
    def v1_retorna_entidades_de_nodo(tipo: str = "Tipo nodo", nombre: str = "Nombre nodo",
                                  entidad_tipo: str = "Entidad tipo",
                                  entidad_nombre: str = "Entidad nombre", response: Response = Response()):
        """ Retorna las entidades de un nodo """
        resp, response.status_code = get_retorna_entidades_de_nodo(tipo, nombre, entidad_tipo,
                                                                   entidad_nombre, version=V1_SR_NODE_LABEL)
        return resp

    @router.get('/nodos')
    @router.get('/nodos/')
    @router.get('/nodos/{filter_str}')
    def v1_muestra_todos_los_nombres_nodos_existentes(filter_str=None, response: Response = Response()):
        """
        Muestra todos los nombres de los nodos existentes si el filtro está vacio \n
        Los caracteres * son comodines de busqueda \n
        Ejemplo: ['pala', 'alambre', 'pétalo'] , \n
                *ala* => 'pala', 'alambre'  \n
                ala* => 'alambre'
        """
        resp, response.status_code = get_muestra_todos_los_nombres_nodos_existentes(filter_str, version=V1_SR_NODE_LABEL)
        return resp