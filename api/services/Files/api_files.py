# Created by Roberto Sanchez at 4/16/2019
# -*- coding: utf-8 -*-
""""
    Servicio Web de Archivos:
        - Permite descargar archivos desde el repositorio local del servidor
        - Consultar archivos existentes en ubicación

    If you need more information. Please contact the email above: rg.sanchez.arg_from@gmail.com
    "My work is well done to honor God at any time" R Sanchez A.
    Mateo 6:33
"""
from flask_restplus import Resource
from flask import request, send_from_directory
import re, os
# importando configuraciones iniciales
from settings import initial_settings as init
from api.services.restplus_config import api
from api.services.restplus_config import default_error_handler
from api.services.sRemoto import serializers as srl
from api.services.sRemoto import parsers
# importando clases para leer desde MongoDB
from my_lib.mongo_engine_handler.sRNode import *
from my_lib.utils import group_files

# configurando logger y el servicio web
log = init.LogDefaultConfig("ws_sFiles.log").logger
ns = api.namespace('files', description='Relativas a la administración de archivos estáticos en el servidor')

ser_from = srl.sRemotoSerializers(api)
api = ser_from.add_serializers()


@ns.route('/<string:repo>')
@ns.route('/<string:repo>/<string:agrupado>')
@ns.route('/<string:repo>/<string:agrupado>/')
@ns.route('/<string:repo>/<string:agrupado>/<string:filtrado>')
class FileAPI(Resource):
    def get(self, repo="Nombre del repositorio", agrupado="no-agrupado", filtrado=""):
        """ Trae una lista de los archivos disponibles en el repositorio
            nombre: nombre del repositorio
            agrupado: "agrupado", "no-agrupado"
            fitrado: archivos a filtrar
            Repositorios disponibles: [s_remoto_excel, s_central_excel]
        """
        # Check /settings/config para añadir más repositorios
        try:
            names = [n.split("\\")[-1] for n in init.REPOS]
            if repo not in names:
                return dict(success=False, msg="No existe el repositorio consultado"), 404
            repo = init.FINAL_REPO[names.index(repo)]
            files = [f for f in os.listdir(repo) if os.path.isfile(os.path.join(repo, f))]
            result = [dict(name=file,
                           datetime=str(dt.datetime.fromtimestamp(os.path.getmtime(os.path.join(repo, file)))))
                      for file in files]
            if "no-agrupado" == agrupado.lower():
                return dict(result=result), 200
            if "agrupado" != agrupado.lower():
                return dict(success=False, msg=f"No existe la opción: {agrupado}"), 404
            # agrupar archivos por nombre y ordenar
            result = group_files(repo, files)
            if filtrado == "":
                return dict(result=result), 200
            filter_dict = dict()
            for k in result.keys():
                if filtrado.lower() in k.lower() or k.lower() in filtrado.lower():
                    filter_dict[k] = result[k]
            return dict(result=filter_dict), 200

        except Exception as e:
            return default_error_handler(e)



@ns.route('/file/<string:repo>/<string:nombre>')
class FileDownloadAPI(Resource):

    def get(self, repo="Nombre del repositorio", nombre="Nombre del archivo"):
        """
            Descarga un archivo de un repositorio
            repo: Nombre del repositorio [s_remoto_excel, s_central_excel]
            nombre: Nombre del archivo
        """
        try:
            names = [n.split("\\")[-1] for n in init.REPOS]
            if repo not in names:
                return dict(success=False, msg="No existe el repositorio consultado"), 404
            repo = init.FINAL_REPO[names.index(repo)]
            files = [f for f in os.listdir(repo) if os.path.isfile(os.path.join(repo, f))]
            files = [str(file).lower() for file in files]
            if nombre.lower() not in files:
                return dict(success=False, msg="No existe el archivo buscado")

            return send_from_directory(repo, nombre, as_attachment=False)

        except Exception as e:
            return default_error_handler(e)