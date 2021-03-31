# Created by Roberto Sanchez at 4/16/2019
# -*- coding: utf-8 -*-
""""
    Servicio Web de Sistema Remoto:
        - Permite configurar los reportes

    If you need more information. Please contact the email above: rg.sanchez.a@gmail.com
    "My work is well done to honor God at any time" R Sanchez A.
    Mateo 6:33
"""

from flask_restplus import Resource
from flask import request, send_from_directory
import re, os
# importando configuraciones iniciales
from dto.classes.StoppableThreadReport import ReportGenerator
from dto.mongo_engine_handler.ProcessingState import TemporalProcessingStateReport
from dto.mongo_engine_handler.SRFinalReport.SRFinalReportTemporal import SRFinalReportTemporal
from settings import initial_settings as init
from api.services.restplus_config import api
from api.services.restplus_config import default_error_handler
from api.services.Reports import serializers as srl
from api.services.sRemoto import parsers
# importando clases para leer desde MongoDB
from dto.mongo_engine_handler.sRNode import *
from motor.master_scripts.eng_sRmaster import *
import threading

# configurando logger y el servicio web
import my_lib.utils as u

ns = api.namespace('admin-report', description='Administración/Configuración de reportes')

ser_from = srl.Serializers(api)
api = ser_from.add_serializers()


@ns.route('/acumulado/<string:report_id>')
class AcumuladoAPI(Resource):
    @api.expect(ser_from.report_config)
    def put(self, report_id):
        """ Configuración del reporte acumulado """

        request_data = dict(request.json)
        state_report = TemporalProcessingStateReport.objects(id_report=report_id).first()
        if state_report is not None:
            state_report.update_time_parameters(info=request_data)
        return True


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


@ns.route('/run/reporte/diario/<string:ini_date>/<string:end_date>')
class ExecuteDailyAPI(Resource):
    def put(self, ini_date: str = "yyyy-mm-dd", end_date: str = "yyyy-mm-dd"):
        # realizando el cálculo por cada nodo:
        success1, ini_date = u.check_date_yyyy_mm_dd(ini_date)
        success2, end_date = u.check_date_yyyy_mm_dd(end_date)
        if not success1 or not success2:
            msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
            return dict(success=False, msg=msg), 400
        date_range = pd.date_range(start=ini_date, end=end_date, freq=dt.timedelta(days=1))
        executing_reports = list()
        existing_reports = list()
        for ini, end in zip(date_range, date_range[1:]):
            report = SRFinalReportTemporal.objects(fecha_inicio=ini, fecha_final=end).first()
            if report is None:
                p = threading.Thread(target=run_nodes_and_summarize, kwargs={"report_ini_date": ini,
                                                                             "report_end_date": end,
                                                                             "save_in_db": True, "force": True})
                p.start()
                executing_reports.append([str(ini), str(end)])
            else:
                existing_reports.append([str(ini), str(end)])
        return dict(success=True, existing_reports=existing_reports, executing_reports=executing_reports), 200

