# Created by Roberto Sanchez at 4/16/2019
# -*- coding: utf-8 -*-
from fastapi import APIRouter
from starlette.responses import Response

from app.core.config import Settings
from app.schemas.RequestSchemas import RoutineOptions
from app.services.AdminReport.AdminReportService import *

# Administración/Configuración de reportes
router = APIRouter(
    prefix=f"{Settings.API_PREFIX}/admin-report",
    tags=["admin-report"],
    responses={404: {"description": "Not found"}},
)

""""
    Servicio Web de Sistema Remoto:
        - Permite configurar los reportes

    If you need more information. Please contact the email above: rg.sanchez.a@gmail.com
    "My work is well done to honor God at any time" R Sanchez A.
    Mateo 6:33
"""


@router.put('/config/{id_report}', status_code=200)
def configuracion_para_ejecucion_reporte(id_report: str = "ID del nodo a cambiar", request_data: ConfigReport = None,
                                         response: Response = Response()):
    """ Configuración para la ejecución del reporte """
    resp, response.status_code = put_configuracion_para_ejecucion_reporte(id_report, request_data)
    return resp


run_endpoint_by_report_id = '/run/routine/report/{id_report}'


@router.post(run_endpoint_by_report_id, status_code=200)
def corre_rutinariamente_un_reporte_por_id(id_report: RoutineOptions, response: Response = Response()):
    """ Corre de manera rutinaria el reporte con el id """
    resp, response.status_code = post_corre_rutinariamente_un_reporte_por_id(id_report)
    return resp


@router.delete(run_endpoint_by_report_id, status_code=200)
def detiene_rutina_en_ejecucion(id_report: RoutineOptions, response: Response = Response()):
    """ Detiene la rutina que este en ejecución, si no está en ejecución entonces 404 """
    resp, response.status_code = delete_detiene_rutina_en_ejecucion(id_report)
    return resp


@router.put(run_endpoint_by_report_id, status_code=200)
def renicia_rutina_en_ejecucion(id_report, response: Response = Response()):
    """ Reinicia la rutina <id_report> """
    resp, response.status_code = put_renicia_rutina_en_ejecucion(id_report)
    return resp


@router.get('/check/reporte/diario/{ini_date}/{end_date}')
def identifica_reportes_existentes(ini_date: str = "yyyy-mm-dd H:M:S", end_date: str = "yyyy-mm-dd H:M:S",
                                   response: Response = Response()):
    """ Permite identificar los reportes existentes  """
    resp, response.status_code = get_identifica_reportes_existentes(ini_date, end_date)
    return resp


@router.put('/run/reporte/diario')
@router.put('/run/reporte/diario/{ini_date}/{end_date}')
def ejecuta_reportes_diarios(ini_date: str = None, end_date: str = None, response: Response = Response()):
    """ Ejecuta reportes diarios desde fecha inicial a final \n
        Fecha inicial formato:  <b>yyyy-mm-dd, yyyy-mm-dd H:M:S</b> \n
        Fecha final formato:    <b>yyyy-mm-dd, yyyy-mm-dd H:M:S</b>
    """
    resp, response.status_code = put_ejecuta_reportes_diarios(ini_date, end_date)
    return resp


@router.delete('/reporte/diario/{ini_date}/{end_date}')
def elimina_reportes_diarios_en_rango(ini_date: str = None, end_date: str = None, response: Response = Response()):
    """ Elimina reportes diarios desde fecha inicial a final \n
        Fecha inicial formato:  <b>yyyy-mm-dd, yyyy-mm-dd H:M:S</b> \n
        Fecha final formato:    <b>yyyy-mm-dd, yyyy-mm-dd H:M:S</b>
    """
    resp, response.status_code = delete_elimina_reportes_diarios_en_rango(ini_date, end_date)
    return resp
