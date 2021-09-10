# Created by Roberto Sanchez at 4/16/2019
# -*- coding: utf-8 -*-
""""
    Servicio Web de Sistema Remoto:
        - Permite configurar los reportes

    If you need more information. Please contact the email above: rg.sanchez.a@gmail.com
    "My work is well done to honor God at any time" R Sanchez A.
    Mateo 6:33
"""
import time

from flask_restplus import Resource
from flask import request
# importando configuraciones iniciales
from flask_app.dto.classes.StoppableThreadDailyReport import StoppableThreadDailyReport
from flask_app.dto.classes.StoppableThreadMailReport import StoppableThreadMailReport
from flask_app.dto.classes.utils import get_thread_by_name
from flask_app.api.services.restplus_config import api
from flask_app.api.services.CustomReports import serializers as srl
# importando clases para leer desde MongoDB
from flask_app.dto.mongo_engine_handler.ProcessingState import TemporalProcessingStateReport
from flask_app.dto.mongo_engine_handler.SRFinalReport.SRFinalReportTemporal import SRFinalReportTemporal
from flask_app.dto.mongo_engine_handler.sRNode import *
import threading

# configurando logger y el servicio web
import flask_app.my_lib.utils as u
from flask_app.motor.master_scripts.eng_sRmaster import run_nodes_and_summarize

ns = api.namespace('admin-report', description='Administración/Configuración de reportes')

ser_from = srl.Serializers(api)
api = ser_from.add_serializers()


@ns.route('/config/<string:id_report>')
class ConfigRoutineReportAPI(Resource):
    @api.expect(ser_from.report_config)
    def put(self, id_report):
        """ Configuración para la ejecución del reporte """
        request_data = dict(request.json)
        state_report = TemporalProcessingStateReport.objects(id_report=id_report).first()
        if state_report is not None:
            state_report.update(info=request_data)
        else:
            state_report = TemporalProcessingStateReport(id_report=id_report, info=request_data, msg="Rutina configurada")
            state_report.save()
        return dict(success=True, msg="Parámetros configurados de manera correcta"), 200


@ns.route('/run/routine/report/<string:id_report>')
class RunRoutineReportAPI(Resource):
    def post(self, id_report):
        """ Corre de manera rutinaria el reporte con el id """
        th = get_thread_by_name(id_report)
        if th is None:
            if id_report == 'rutina_de_reporte_diario':
                state = TemporalProcessingStateReport.objects(id_report=id_report).first()
                if state is None:
                    return dict(success=False, msg="La rutina aún no ha sido configurada"), 404
                trigger = dt.timedelta(**state.info["trigger"])
                th_v = StoppableThreadDailyReport(trigger=trigger, name=id_report)
                th_v.start()
                return dict(success=True, msg="La rutina ha sido inicializada"), 200
            if id_report == 'rutina_correo_electronico':
                state = TemporalProcessingStateReport.objects(id_report=id_report).first()
                if state is None:
                    return dict(success=False, msg="La rutina aún no ha sido configurada"), 404
                trigger = dt.timedelta(**state.info["trigger"])
                mail_config = state.info["mail_config"]
                parameters = state.info["parameters"]
                th_v = StoppableThreadMailReport(name=id_report, trigger=trigger, mail_config=mail_config,
                                                 parameters=parameters)
                th_v.start()
                return dict(success=True, msg="La rutina ha sido inicializada"), 200
        return dict(success=False, msg="La rutina ya se encuentra en ejecución"), 409

    def delete(self, id_report):
        """ Detiene la rutina que este en ejecución, si no está en ejecución entonces 404 """
        th = get_thread_by_name(id_report)
        if th is None:
            return dict(success=False, msg="Esta rutina no está en ejecución"), 404
        th.stop()
        time.sleep(th.seconds_to_sleep/2)
        return dict(success=True, msg="La rutina ha sido detenida"), 200

    def put(self, id_report):
        """ Reinicia la rutina <id_report> """
        self.delete(id_report)
        return self.post(id_report)



@ns.route('/check/reporte/diario/<string:ini_date>/<string:end_date>')
class CheckDailyAPI(Resource):
    def get(self, ini_date: str = "yyyy-mm-dd H:M:S", end_date: str = "yyyy-mm-dd H:M:S"):
        """ Permite identificar los reportes existentes  """
        success1, ini_date = u.check_date_yyyy_mm_dd(ini_date)
        success2, end_date = u.check_date_yyyy_mm_dd(end_date)
        if not success1 or not success2:
            msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
            return dict(success=False, msg=msg), 400
        span = dt.timedelta(days=1)
        date_range = pd.date_range(start=ini_date, end=end_date, freq=span)
        missing = list()
        done = list()
        for ini, end in zip(date_range, date_range[1:]):
            report = SRFinalReportTemporal.objects(fecha_inicio=ini, fecha_final=end).first()
            missing.append([str(ini), str(end)]) if report is None else done.append([str(ini), str(end)])
        is_ok = len(done) == len(date_range[1:])
        msg = "Todos los reportes existen en base de datos" if is_ok else "Faltan reportes en base de datos"
        return dict(success=is_ok, done_reports=done, missing_reports=missing, msg=msg), 200 if is_ok else 404


@ns.route('/run/reporte/diario')
@ns.route('/run/reporte/diario/<string:ini_date>/<string:end_date>')
class ExecuteDailyAPI(Resource):

    def put(self, ini_date: str = None, end_date: str = None):
        """ Ejecuta reportes diarios desde fecha inicial a final
            Fecha inicial formato:  <b>yyyy-mm-dd, yyyy-mm-dd H:M:S</b>
            Fecha final formato:    <b>yyyy-mm-dd, yyyy-mm-dd H:M:S</b>
        """
        if ini_date is None and end_date is None:
            ini_date, end_date = u.get_dates_by_default()
        else:
            success1, ini_date = u.check_date_yyyy_mm_dd(ini_date)
            success2, end_date = u.check_date_yyyy_mm_dd(end_date)
            if not success1 or not success2:
                msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
                return dict(success=False, msg=msg), 400
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
        return dict(success=True, existing_reports=existing_reports, executing_reports=executing_reports), 200


@ns.route('/reporte/diario/<string:ini_date>/<string:end_date>')
class ExecuteDailyAPI(Resource):

    def delete(self, ini_date: str = None, end_date: str = None):
        """ Elimina reportes diarios desde fecha inicial a final
            Fecha inicial formato:  <b>yyyy-mm-dd, yyyy-mm-dd H:M:S</b>
            Fecha final formato:    <b>yyyy-mm-dd, yyyy-mm-dd H:M:S</b>
        """
        success1, ini_date = u.check_date_yyyy_mm_dd(ini_date)
        success2, end_date = u.check_date_yyyy_mm_dd(end_date)
        if not success1 or not success2:
            msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
            return dict(success=False, msg=msg), 400
        date_range = pd.date_range(start=ini_date, end=end_date, freq=dt.timedelta(days=1))
        deleted_reports = list()
        for ini, end in zip(date_range, date_range[1:]):
            report = SRFinalReportTemporal.objects(fecha_inicio=ini, fecha_final=end).first()
            if report is not None:
                report.delete()
                deleted_reports.append([str(ini), str(end)])
        return dict(success=True, deleted_reports=deleted_reports), 200


def executing_all_reports(to_execute_reports):
    # realizando el cálculo por cada nodo:
    for ini, end in to_execute_reports:
        run_nodes_and_summarize(ini, end,save_in_db=True, force=True)
