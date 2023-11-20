from fastapi import APIRouter
from starlette.responses import Response

from app.core.config import Settings
from app.services.SRemoto.SRemotoService import *

router = APIRouter(
    prefix=f"{Settings.API_PREFIX}/sRemoto",
    tags=["sRemoto"],
    responses={404: {"description": "Not found"}},
)

disponibilidad_excel_uri = '/disponibilidad/{formato}/{ini_date}/{end_date}'


@router.get(disponibilidad_excel_uri)
def descarga_calculo_en_excel(formato: FormatOption, ini_date: str = "yyyy-mm-dd H:M:S",
                              end_date: str = "yyyy-mm-dd H:M:S",
                              response: Response = Response()):
    """ Entrega el cálculo en formato Excel/JSON realizado por acciones POST/PUT \n
        Si el cálculo no existe entonces <b>código 404</b>  \n
        Formato:                excel, json \n
        Fecha inicial formato:  <b>yyyy-mm-dd, yyyy-mm-dd H:M:S</b> \n
        Fecha final formato:    <b>yyyy-mm-dd, yyyy-mm-dd H:M:S</b>
    """
    resp, response.status_code = get_descarga_calculo_en_excel(formato, ini_date, end_date)
    return resp


indisponibilidad__format_uri = '/indisponibilidad/tags/{formato}'


@router.get(indisponibilidad__format_uri)
@router.get(indisponibilidad__format_uri + '/{ini_date}/{end_date}')
@router.get(indisponibilidad__format_uri + '/{ini_date}/{end_date}/{umbral}')
def obtiene_listado_tags_con_indisponibilidad_mayor_igual_a_umbral(formato: FormatOption, ini_date: str = None,
                                                                   end_date: str = None,
                                                                   umbral=None, response: Response = Response()):
    """
    Entrega el listado de tags cuya indisponibilidad sea mayor igual al umbral (por defecto 0) \n
    Si el cálculo no existe entonces <b>código 404</b> \n
    Formato:                excel, json \n
    Fecha inicial formato:  <b>yyyy-mm-dd, yyyy-mm-dd H:M:S</b> \n
    Fecha final formato:    <b>yyyy-mm-dd, yyyy-mm-dd H:M:S</b> \n
    Umbral:                 <b>float</b> \n
    Se puede consultar este servicio como: /url?nid=<cualquier_valor_random>
    """
    resp, response.status_code = get_obtiene_listado_tags_con_indisponibilidad_mayor_igual_a_umbral(
        formato, ini_date, end_date, umbral
    )
    return resp


disponibilidad_diaria_uri = '/disponibilidad/diaria/{formato}'


@router.get(disponibilidad_diaria_uri)
@router.get(disponibilidad_diaria_uri + '/{ini_date}/{end_date}')
def obtiene_calculo_diario(formato: FormatOption, ini_date=None, end_date=None, response: Response = Response()):
    """
    Entrega el cálculo en formato Excel/JSON realizado de manera diaria a las 00:00 \n
    Si el cálculo no existe entonces <b>código 404</b> \n
    Formato:                excel, json \n
    Fecha inicial formato:  <b>yyyy-mm-dd</b> \n
    Fecha final formato:    <b>yyyy-mm-dd</b>
    """
    resp, response.status_code = get_obtiene_calculo_diario(formato, ini_date, end_date)
    return resp


tendencia_diaria_uri = '/tendencia/diaria/{formato}/{ini_date}/{end_date}'


@router.get(tendencia_diaria_uri)
def obtiene_tendencia_reporte_diario(formato: FormatOption, ini_date=None, end_date=None, response: Response = Response()):
    """
    Entrega la disponibilidad de datos en formato Excel/JSON realizado de manera diaria a las 00:00 \n
    Si el cálculo no existe entonces <b>código 404</b> \n
    Formato:                excel, json \n
    Fecha inicial formato:  <b>yyyy-mm-dd</b> \n
    Fecha final formato:    <b>yyyy-mm-dd</b>
    """
    resp, response.status_code = get_obtiene_tendencia_reporte_diario(formato, ini_date, end_date)
    return resp
