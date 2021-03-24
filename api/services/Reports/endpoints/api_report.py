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
from dto.mongo_engine_handler.ProcessingState import TemporalProcessingStateReport
from settings import initial_settings as init
from api.services.restplus_config import api
from api.services.restplus_config import default_error_handler
from api.services.Reports import serializers as srl
from api.services.sRemoto import parsers
# importando clases para leer desde MongoDB
from dto.mongo_engine_handler.sRNode import *
from random import randint

# configurando logger y el servicio web
log = init.LogDefaultConfig("ws_report.log").logger
ns = api.namespace('admin-report', description='Administración/Configuración de reportes')

ser_from = srl.Serializers(api)
api = ser_from.add_serializers()



@ns.route('/acumulado/<string:report_id>')
class AcumuladoAPI(Resource):
    @api.expect(ser_from.report_config)
    def put(self, report_id):
        """ Configuración del reporte acumulado """
        try:
            request_data = dict(request.json)
            state_report = TemporalProcessingStateReport.objects(id_report=report_id).first()
            if state_report is not None:
                state_report.update_time_parameters(info=request_data)
            return True
        except Exception as e:
            return default_error_handler(e)



