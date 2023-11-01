from fastapi import APIRouter
from starlette.responses import Response

from app.core.config import Settings
from app.endpoints.AdminSRemoto.Excel import node_from_excel_endpoints
from app.endpoints.AdminSRemoto.NodeById import node_id_endpoints
from app.endpoints.AdminSRemoto.NodeByTypeAndName import node_type_and_name_endpoints
from app.endpoints.AdminSRemoto.RTU import rtu_endpoints
from app.endpoints.AdminSRemoto.Tags import tags_endpoints
from app.services.AdminSRemoto.AdminSRemotoService import *

router = APIRouter(
    prefix=f"{Settings.API_PREFIX}/admin-sRemoto",
    tags=["admin-sRemoto"],
    responses={404: {"description": "Not found"}},
)


def create_grouped_endpoints(_router: APIRouter):
    node_id_endpoints(_router)
    node_type_and_name_endpoints(_router)
    node_from_excel_endpoints(_router)
    rtu_endpoints(_router)
    tags_endpoints(router)


@router.put('/nodo/id/{id_nodo}/activado', status_code=200)
def activa_nodo(id_nodo: str = "ID del nodo a cambiar", response: Response = Response()):
    resp, response.status_code = put_activa_nodo(id_nodo)
    return resp


@router.put('/nodo/id/{id_nodo}/desactivado', status_code=200)
def desactiva_nodo(id_nodo: str = "ID del nodo a cambiar", response: Response = Response()):
    resp, response.status_code = put_desactiva_nodo(id_nodo)
    return resp


@router.get('/nodo/{tipo}/{nombre}/{entidad_tipo}/{entidad_nombre}')
def retorna_entidades_de_nodo(tipo: str = "Tipo nodo", nombre: str = "Nombre nodo", entidad_tipo: str = "Entidad tipo",
                              entidad_nombre: str = "Entidad nombre", response: Response = Response()):
    """ Retorna las entidades de un nodo """
    resp, response.status_code = get_retorna_entidades_de_nodo(tipo, nombre, entidad_tipo, entidad_nombre)
    return resp


@router.get('/nodos/')
@router.get('/nodos/{filter_str}')
def muestra_todos_los_nombres_nodos_existentes(filter_str=None, response: Response = Response()):
    """
    Muestra todos los nombres de los nodos existentes si el filtro está vacio \n
    Los caracteres * son comodines de busqueda \n
    Ejemplo: ['pala', 'alambre', 'pétalo'] , \n
            *ala* => 'pala', 'alambre'  \n
            ala* => 'alambre'
    """
    resp, response.status_code = get_muestra_todos_los_nombres_nodos_existentes(filter_str)
    return resp


