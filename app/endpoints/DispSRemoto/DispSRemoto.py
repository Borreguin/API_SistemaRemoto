from fastapi import APIRouter
from starlette.responses import Response

from app.core.config import Settings
from app.services.DispSRemoto.DispSRemotoService import *

router = APIRouter(
    prefix=f"{Settings.API_PREFIX}/disp-sRemoto",
    tags=["disp-sRemoto"],
    responses={404: {"description": "Not found"}},
)

disponibilidad_date_range = '/disponibilidad/{ini_date}/{end_date}'


@router.put(disponibilidad_date_range)
def calcula_o_sobreescribe_disponibilidad_en_rango_fecha(ini_date: str = "yyyy-mm-dd H:M:S",
                                                         end_date: str = "yyyy-mm-dd H:M:S",
                                                         response: Response = Response()):
    """ Calcula/sobre-escribe la disponibilidad de los nodos que se encuentren activos en base de datos \n
        Si ya existe reportes asociados a los nodos, estos son <b>recalculados</b> \n
        Este proceso puede tomar para su ejecución al menos <b>20 minutos</b> \n
        Fecha inicial formato:  <b>yyyy-mm-dd H:M:S</b> \n
        Fecha final formato:    <b>yyyy-mm-dd H:M:S</b>
    """
    resp, response.status_code = put_calcula_o_sobreescribe_disponibilidad_en_rango_fecha(ini_date, end_date)
    return resp


@router.post(disponibilidad_date_range)
def calcula_disponibilidad_en_rango_fecha(ini_date: str = "yyyy-mm-dd H:M:S", end_date: str = "yyyy-mm-dd H:M:S",
                                          response: Response = Response()):
    """ Calcula si no existe la disponibilidad de los nodos que se encuentren activos en base de datos \n
        Si ya existe reportes asociados a los nodos, estos <b>no son recalculados</b> \n
        Este proceso puede tomar para su ejecución al menos <b>20 minutos</b> \n
        Fecha inicial formato:  <b>yyyy-mm-dd H:M:S</b> \n
        Fecha final formato:    <b>yyyy-mm-dd H:M:S</b>
    """
    resp, response.status_code = post_calcula_disponibilidad_en_rango_fecha(ini_date, end_date)
    return resp


@router.get(disponibilidad_date_range)
def obtiene_disponibilidad_en_rango_fecha(ini_date: str = "yyyy-mm-dd H:M:S", end_date: str = "yyyy-mm-dd H:M:S",
                                          response: Response = Response()):
    """ Entrega el cálculo realizado por acciones POST/PUT \n
        Si el cálculo no existe entonces <b>código 404</b> \n
        Fecha inicial formato:  <b>yyyy-mm-dd H:M:S</b> \n
        Fecha final formato:    <b>yyyy-mm-dd H:M:S</b>
    """
    resp, response.status_code = get_obtiene_disponibilidad_en_rango_fecha(ini_date, end_date)
    return resp


disponibilidad_by_nodo_and_date_range = '/disponibilidad/nodo/{ini_date}/{end_date}'


@router.put(disponibilidad_by_nodo_and_date_range)
def calcula_o_sobrescribe_disponibilidad_nodo_en_rango_fecha(ini_date: str = "yyyy-mm-dd H:M:S",
                                                             end_date: str = "yyyy-mm-dd H:M:S",
                                                             request_data: NodeRequest = NodeRequest(),
                                                             response: Response = Response()):
    """ Calcula/sobre-escribe la disponibilidad de un nodo especificado por tipo y nombre \n
        Si ya existe reporte asociados al nodo, este es <b>recalculado</b> \n
        Fecha inicial formato:  <b>yyyy-mm-dd H:M:S</b> \n
        Fecha final formato:    <b>yyyy-mm-dd H:M:S</b>
    """
    resp, response.status_code = put_calcula_o_sobrescribe_disponibilidad_nodo_en_rango_fecha(ini_date, end_date,
                                                                                              request_data)
    return resp


disponibilidad_by_nodos_and_date_range = '/disponibilidad/nodos/{ini_date}/{end_date}'


@router.put(disponibilidad_by_nodos_and_date_range)
def calcula_o_sobrescribe_disponibilidad_nodos_en_lista(ini_date: str = "yyyy-mm-dd H:M:S",
                                                        end_date: str = "yyyy-mm-dd H:M:S",
                                                        request_data: NodesRequest = NodesRequest(),
                                                        response: Response = Response()):
    """ Calcula/sobre-escribe la disponibilidad de los nodos especificados en la lista \n
        Si ya existe reportes asociados a los nodos, estos son <b>recalculados</b> \n
        Fecha inicial formato:  <b>yyyy-mm-dd H:M:S</b> \n
        Fecha final formato:    <b>yyyy-mm-dd H:M:S</b>
    """
    resp, response.status_code = put_calcula_o_sobrescribe_disponibilidad_nodos_en_lista(ini_date, end_date,
                                                                                         request_data)
    return resp


@router.post(disponibilidad_by_nodos_and_date_range)
def calcula_disponibilidad_nodos_en_lista(ini_date: str = "yyyy-mm-dd H:M:S", end_date: str = "yyyy-mm-dd H:M:S",
                                          request_data: NodesRequest = NodesRequest(),
                                          response: Response = Response()):
    """ Calcula si no existe la disponibilidad de los nodos especificados en la lista \n
        Si ya existe reportes asociados a los nodos, estos <b>no son recalculados</b> \n
        Fecha inicial formato:  <b>yyyy-mm-dd H:M:S</b> \n
        Fecha final formato:    <b>yyyy-mm-dd H:M:S</b>
    """
    resp, response.status_code = post_calcula_disponibilidad_nodos_en_lista(ini_date, end_date, request_data)
    return resp


@router.delete(disponibilidad_by_nodos_and_date_range)
def elimina_disponibilidad_de_nodos_en_lista(ini_date: str = "yyyy-mm-dd H:M:S", end_date: str = "yyyy-mm-dd H:M:S",
                                             request_data: NodesRequest = NodesRequest(),
                                             response: Response = Response()):
    """ Elimina si existe la disponibilidad de los nodos especificados en la lista \n
        Fecha inicial formato:  <b>yyyy-mm-dd H:M:S</b> \n
        Fecha final formato:    <b>yyyy-mm-dd H:M:S</b>
    """
    resp, response.status_code = delete_elimina_disponibilidad_de_nodos_en_lista(ini_date, end_date, request_data)
    return resp


disponibilidad_by_tipo_nodo_and_date_range = '/disponibilidad/{tipo}/{nombre}/{ini_date}/{end_date}'


@router.get(disponibilidad_by_tipo_nodo_and_date_range)
def obtiene_disponibilidad_por_tipo_nodo(tipo="tipo de nodo", nombre="nombre del nodo",
                                         ini_date: str = "yyyy-mm-dd H:M:S",
                                         end_date: str = "yyyy-mm-dd H:M:S", response: Response = Response()):
    """ Obtiene el reporte de disponibilidad de los nodos especificados en la lista \n
        Si el reporte no existe, entonces su valor será 404 \n
        Fecha inicial formato:  <b>yyyy-mm-dd H:M:S</b> \n
        Fecha final formato:    <b>yyyy-mm-dd H:M:S</b>
    """
    resp, response.status_code = get_obtiene_disponibilidad_por_tipo_nodo(tipo, nombre, ini_date, end_date)
    return resp


@router.delete(disponibilidad_by_tipo_nodo_and_date_range)
def elimina_reporte_disponibilidad_de_nodo(tipo="tipo de nodo", nombre="nombre del nodo",
                                           ini_date: str = "yyyy-mm-dd H:M:S",
                                           end_date: str = "yyyy-mm-dd H:M:S", response: Response = Response()):
    """ Elimina el reporte de disponibilidad del nodo especificado \n
        Si el reporte no existe, entonces código 404 \n
        Fecha inicial formato:  <b>yyyy-mm-dd H:M:S</b> \n
        Fecha final formato:    <b>yyyy-mm-dd H:M:S</b>
    """
    resp, response.status_code = delete_elimina_reporte_disponibilidad_de_nodo(tipo, nombre, ini_date, end_date)
    return resp


disponibilidad_by_report_id = '/disponibilidad/nodo/{id_report}'


@router.get(disponibilidad_by_report_id)
def obtiene_reporte_disponibilidad_por_id_reporte(id_report="Id del reporte de detalle",
                                                  response: Response = Response()):
    """ Obtiene el reporte de disponibilidad del nodo de acuerdo al id del reporte \n
        Si el reporte no existe, entonces su valor será 404
    """
    resp, response.status_code = get_obtiene_reporte_disponibilidad_por_id_reporte(id_report)
    return resp


disponibilidad_detalles_by_report_id = '/disponibilidad/detalles/{id_report}'


@router.get(disponibilidad_detalles_by_report_id)
def obtiene_detalle_reporte_disponibilidad(id_report="Id del reporte de detalle", response: Response = Response()):
    """ Obtiene el reporte de disponibilidad del nodo de acuerdo al id del reporte \n
        Si el reporte no existe, entonces su valor será 404
    """
    resp, response.status_code = get_obtiene_detalle_reporte_disponibilidad(id_report)
    return resp


status_disponibilidad_time_range = '/estado/disponibilidad/{ini_date}/{end_date}'


@router.get(status_disponibilidad_time_range)
def obtiene_estado_calculo_reporte(ini_date: str = "yyyy-mm-dd H:M:S", end_date: str = "yyyy-mm-dd H:M:S",
                                   response: Response = Response()):
    """ Obtiene el estado del cálculo de los reportes de disponibilidad existentes en el periodo especificado \n
        Nota: Este servicio es válido solamente al momento de consultar el estado de un cálculo en proceso \n
        en otro contexto, este servicio no garantiza un comportamiento correcto \n
        Fecha inicial formato:  <b>yyyy-mm-dd H:M:S</b> \n
        Fecha final formato:    <b>yyyy-mm-dd H:M:S</b>
    """
    resp, response.status_code = get_obtiene_estado_calculo_reporte(ini_date, end_date)
    return resp
