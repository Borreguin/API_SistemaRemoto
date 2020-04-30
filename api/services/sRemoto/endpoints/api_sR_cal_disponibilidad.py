# Created by Roberto Sanchez at 4/16/2019
# -*- coding: utf-8 -*-
""""
    Servicio Web de Sistema Remoto (cálculo de disponibilidad):
        - Permite el cálculo de disponibilidad de todos los nodos
        - Permite el cálculo de disponibilidad por nodos

    If you need more information. Please contact the email above: rg.sanchez.arg_from@gmail.com
    "My work is well done to honor God at any time" R Sanchez A.
    Mateo 6:33
"""
from flask_restplus import Resource
from werkzeug.exceptions import BadRequest
from flask import request
import re, os
# importando configuraciones iniciales
from settings import initial_settings as init
from api.services.restplus_config import api
from api.services.restplus_config import default_error_handler
import my_lib.utils as u
from api.services.sRemoto import serializers as srl
from api.services.sRemoto import parsers
# importando clases para leer desde MongoDB
from my_lib.mongo_engine_handler.sRNode import *

# configurando logger y el servicio web
log = init.LogDefaultConfig("ws_sRemoto.log").logger
ns = api.namespace('disp-sRemoto', description='Relativas a Sistema Remoto')


@api.errorhandler(Exception)
@ns.route('/disponibilidad/<string:ini_date>/<string:end_date>')
class Disponibilidad(Resource):
    def put(self, ini_date: str = "yyyy-mm-dd", end_date: str = "yyyy-mm-dd"):
        """ Busca si un nodo tipo SRNode existe en base de datos """
        try:
            success1, ini_date = u.check_date_yyyy_mm_dd(ini_date)
            success2, end_date = u.check_date_yyyy_mm_dd(end_date)
            if not success1 or not success2:
                msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
                return dict(success=False, errors=msg), 400
            nodos = SRNode.objects(nombre="CELEC")
            print(nodos)
        except Exception as e:
            return default_error_handler(e)
