# Created by Roberto Sanchez at 4/16/2019
# -*- coding: utf-8 -*-
import threading
import time
import pandas as pd

from starlette import status
import datetime as dt

from app.common.util import to_dict
from app.schemas.RequestSchemas import ConfigReport, RoutineOptions
from flask_app.dto.classes.StoppableThreadDailyReport import StoppableThreadDailyReport
from flask_app.dto.classes.StoppableThreadMailReport import StoppableThreadMailReport
from flask_app.dto.classes.utils import get_thread_by_name
from app.db.v1.ProcessingState import TemporalProcessingStateReport
from app.db.v1.SRFinalReport.SRFinalReportTemporal import SRFinalReportTemporal
from flask_app.motor.master_scripts.eng_sRmaster import run_nodes_and_summarize
from flask_app.my_lib.utils import check_date_yyyy_mm_dd, get_dates_by_default


def put_configuracion_para_ejecucion_reporte(id_report: str, request_data: ConfigReport):
    """ Configuración para la ejecución del reporte """
    request_dict = to_dict(request_data)
    state_report = TemporalProcessingStateReport.objects(id_report=id_report).first()
    if state_report is not None:
        state_report.update(info=request_dict)
    else:
        state_report = TemporalProcessingStateReport(id_report=id_report, info=request_dict, msg="Rutina configurada")
        state_report.save()
    return dict(success=True, msg="Parámetros configurados de manera correcta"), status.HTTP_200_OK


def post_corre_rutinariamente_un_reporte_por_id(id_report: RoutineOptions):
    th = get_thread_by_name(id_report)
    if th is None:
        if id_report == RoutineOptions.RUTINA_DIARIA:
            state = TemporalProcessingStateReport.objects(id_report=id_report).first()
            if state is None:
                return dict(success=False, msg="La rutina aún no ha sido configurada"), status.HTTP_404_NOT_FOUND
            trigger = dt.timedelta(**state.info["trigger"])
            th_v = StoppableThreadDailyReport(trigger=trigger, name=id_report)
            th_v.start()
            return dict(success=True, msg="La rutina ha sido inicializada"), status.HTTP_200_OK
        if id_report == RoutineOptions.RUTINA_CORREO:
            state = TemporalProcessingStateReport.objects(id_report=id_report).first()
            if state is None:
                return dict(success=False, msg="La rutina aún no ha sido configurada"), status.HTTP_404_NOT_FOUND
            trigger = dt.timedelta(**state.info["trigger"])
            mail_config = state.info["mail_config"]
            parameters = state.info["parameters"]
            th_v = StoppableThreadMailReport(name=id_report, trigger=trigger, mail_config=mail_config,
                                             parameters=parameters)
            th_v.start()
            return dict(success=True, msg="La rutina ha sido inicializada"), status.HTTP_200_OK
        return dict(success=False, msg="La rutina no ha sido econtrada"), status.HTTP_404_NOT_FOUND
    return dict(success=False, msg="La rutina ya se encuentra en ejecución"), status.HTTP_409_CONFLICT


def delete_detiene_rutina_en_ejecucion(id_report):
    th = get_thread_by_name(id_report)
    if th is None:
        return dict(success=False, msg="Esta rutina no está en ejecución"), status.HTTP_404_NOT_FOUND
    th.stop()
    time.sleep(th.seconds_to_sleep / 2)
    return dict(success=True, msg="La rutina ha sido detenida"), status.HTTP_200_OK


def put_renicia_rutina_en_ejecucion(id_report):
    delete_detiene_rutina_en_ejecucion(id_report)
    return post_corre_rutinariamente_un_reporte_por_id(id_report)


def get_identifica_reportes_existentes(ini_date: str, end_date: str):
    success1, ini_date = check_date_yyyy_mm_dd(ini_date)
    success2, end_date = check_date_yyyy_mm_dd(end_date)
    if not success1 or not success2:
        msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    span = dt.timedelta(days=1)
    date_range = pd.date_range(start=ini_date, end=end_date, freq=span)
    missing = list()
    done = list()
    for ini, end in zip(date_range, date_range[1:]):
        report = SRFinalReportTemporal.objects(fecha_inicio=ini, fecha_final=end).first()
        missing.append([str(ini), str(end)]) if report is None else done.append([str(ini), str(end)])
    is_ok = len(done) == len(date_range[1:])
    msg = "Todos los reportes existen en base de datos" if is_ok else "Faltan reportes en base de datos"
    return dict(success=is_ok, done_reports=done, missing_reports=missing,
                msg=msg), status.HTTP_200_OK if is_ok else 404


def put_ejecuta_reportes_diarios(ini_date: str = None, end_date: str = None):
    if ini_date is None and end_date is None:
        ini_date, end_date = get_dates_by_default()
    else:
        success1, ini_date = check_date_yyyy_mm_dd(ini_date)
        success2, end_date = check_date_yyyy_mm_dd(end_date)
        if not success1 or not success2:
            msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
            return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    date_range = pd.date_range(start=ini_date, end=end_date, freq=dt.timedelta(days=1))
    to_execute_reports = list()
    existing_reports = list()
    executing_reports = list()
    for ini, end in zip(date_range, date_range[1:]):
        report = SRFinalReportTemporal.objects(fecha_inicio=ini, fecha_final=end).first()
        if report is not None:
            existing_reports.append([str(ini), str(end)])
        else:
            to_execute_reports.append([ini, end])
            executing_reports.append([str(ini), str(end)])

    p = threading.Thread(target=executing_all_reports, kwargs={"to_execute_reports": to_execute_reports})
    p.start()
    return dict(success=True, existing_reports=existing_reports,
                executing_reports=executing_reports), status.HTTP_200_OK


def delete_elimina_reportes_diarios_en_rango(ini_date: str = None, end_date: str = None):
    success1, ini_date = check_date_yyyy_mm_dd(ini_date)
    success2, end_date = check_date_yyyy_mm_dd(end_date)
    if not success1 or not success2:
        msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    date_range = pd.date_range(start=ini_date, end=end_date, freq=dt.timedelta(days=1))
    deleted_reports = list()
    for ini, end in zip(date_range, date_range[1:]):
        report = SRFinalReportTemporal.objects(fecha_inicio=ini, fecha_final=end).first()
        if report is not None:
            report.delete()
            deleted_reports.append([str(ini), str(end)])
    return dict(success=True, deleted_reports=deleted_reports), status.HTTP_200_OK


def executing_all_reports(to_execute_reports):
    # realizando el cálculo por cada nodo:
    for ini, end in to_execute_reports:
        run_nodes_and_summarize(ini, end, save_in_db=True, force=True)
