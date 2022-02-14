# Created by Roberto Sanchez at 7/13/2019
# -*- coding: utf-8 -*-
""""
    Servicio Web de Consignaciones:
        - Permite la administración de consignaciones
        - Serializar e ingresar datos

    If you need more information. Please contact the email above: rg.sanchez.arg_from@gmail.com
    "My work is well done to honor God at any time" R Sanchez A.
    Mateo 6:33
"""
import os
import pandas as pd
from flask import request, send_from_directory
from flask_restplus import Resource
from mongoengine import Q
import flask_app.settings.LogDefaultConfig
from flask_app.api.services.Consignaciones import parsers
from flask_app.api.services.Consignaciones import serializers as srl
from flask_app.api.services.restplus_config import api
# importando clases para leer desde MongoDB
# importando configuraciones iniciales
from flask_app.dto.mongo_engine_handler.Info.Consignment import Consignments, Consignment
from flask_app.my_lib.utils import check_range_yyyy_mm_dd_hh_mm_ss, set_max_age_to_response
from flask_app.settings import initial_settings as init

# configurando logger y el servicio web
log = flask_app.settings.LogDefaultConfig.LogDefaultConfig("ws_Consignaciones.log").logger
ns = api.namespace('admin-consignacion', description='Relativas a la administración de consignaciones')

ser_from = srl.ConsignacionSerializers(api)
api = ser_from.add_serializers()


@ns.route('/consignacion/<string:id_elemento>/<string:ini_date>/<string:end_date>')
class ConsignacionAPI(Resource):

    def get(self, id_elemento: str = "id_elemento", ini_date: str = "yyyy-mm-dd hh:mm:ss",
            end_date: str = "yyyy-mm-dd hh:mm:ss"):
        """ Obtener las consignaciones asociadas del elemento: "id_elemento"
            <b>id_elemento</b> corresponde al elemento a consignar
            formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
        """
        success, ini_date, end_date, msg = check_range_yyyy_mm_dd_hh_mm_ss(ini_date, end_date)
        if not success:
            return dict(success=False, msg=msg), 400

        consignacion = Consignments.objects(id_elemento=id_elemento).first()
        if consignacion is None:
            return dict(success=False, msg="No existen consignaciones asociadas a este elemento"), 404
        consignaciones = consignacion.consignments_in_time_range(ini_date, end_date)
        if len(consignaciones) == 0:
            return dict(success=False, msg="No existen consignaciones en el periodo especificado"), 404
        return dict(success=True, consignaciones=[c.to_dict() for c in consignaciones],
                    msg="Se han encontrado consignaciones asociadas"), 200

    @api.expect(ser_from.detalle_consignacion)
    def post(self, id_elemento: str = "id_elemento", ini_date: str = "yyyy-mm-dd hh:mm:ss",
             end_date: str = "yyyy-mm-dd hh:mm:ss"):
        """ Consignar un elemento asociadas a: "id_elemento"
            <b>id_elemento</b> corresponde al elemento a consignar
             formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
        """
        success, ini_date, end_date, msg = check_range_yyyy_mm_dd_hh_mm_ss(ini_date, end_date)
        if not success:
            return dict(success=False, msg=msg), 400

        if ini_date >= end_date:
            msg = "El rango de fechas es incorrecto. Revise que la fecha inicial sea anterior a fecha final"
            return dict(success=False, msg=msg), 400
        detalle = dict(request.json)
        consignaciones = Consignments.objects(id_elemento=id_elemento).first()
        if consignaciones is None:
            consignaciones = Consignments(id_elemento=id_elemento)
        # Actualizando el elemento referenciado:
        elemento = detalle.pop("elemento", None)
        if elemento is not None:
            consignaciones.elemento = detalle.pop("elemento", None)
        consignacion = Consignment(no_consignacion=detalle["no_consignacion"], fecha_inicio=ini_date,
                                   fecha_final=end_date, detalle=detalle["detalle"], responsable=detalle['responsable'])
        # ingresando consignación y guardando si es exitoso:
        success, msg = consignaciones.insert_consignments(consignacion)
        if success:
            consignaciones.save()
            return dict(success=success, msg=msg)
        else:
            return dict(success=success, msg=msg)


@ns.route('/consignacion/<string:id_elemento>/<string:id_consignacion>')
class ConsignacionDeleteEditAPI(Resource):

    def delete(self, id_elemento: str = "id_elemento", id_consignacion: str = "id_consignacion"):
        """ Elimina la consignación asociadas del elemento: "id_elemento" cuya idenficación es "id_consignacion"
            <b>id_elemento</b> corresponde al elemento consignado
            <b>id_consignacion</b> corresponde a la identificación de la consignación
        """
        consignaciones = Consignments.objects(id_elemento=id_elemento).first()
        if consignaciones is None:
            return dict(success=False, msg="No existen consignaciones para este elemento. "
                                           "El elemento no existe"), 404

        # eliminando consignación por id
        success, msg = consignaciones.delete_consignment_by_id(id_consignacion)
        if success:
            consignaciones.save()
            return dict(success=success, msg=msg)
        else:
            return dict(success=success, msg=msg)

    @api.expect(ser_from.consignacion)
    def put(self, id_elemento: str = "id_elemento", id_consignacion: str = "id_consignacion"):
        """ Edita la consignación asociada al elemento: "id_elemento", cuya idenficación es "id_consignacion"
            <b>id_elemento</b> corresponde al elemento consignado
            <b>id_consignacion</b> corresponde a la identificación de la consignación
            formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
        """
        detalle = dict(request.json)
        elemento = detalle.pop("elemento", None)
        consignaciones = Consignments.objects(id_elemento=id_elemento).first()
        if consignaciones is None:
            return dict(success=False, msg="No existen consignaciones para este elemento. "
                                           "El elemento no existe"), 404

        if elemento is not None:
            consignaciones.elemento = elemento

        # editando consignación por id
        consignacion = Consignment(**detalle)
        success, msg = consignaciones.edit_consignment_by_id(id_to_edit=id_consignacion, consignment=consignacion)
        if success:
            consignaciones.save()
            return dict(success=success, msg=msg), 200
        else:
            return dict(success=success, msg=msg), 404

    @api.expect(parsers.consignacion_upload)
    def post(self, id_elemento: str = "id_elemento", id_consignacion: str = "id_consignacion"):
        """ Carga un archivo a la consignación asociada al elemento: "id_elemento", cuya idenficación es "id_consignacion"
                    <b>id_elemento</b> corresponde al elemento consignado
                    <b>id_consignacion</b> corresponde a la identificación de la consignación
                    formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
        """
        args = parsers.consignacion_upload.parse_args()
        consignaciones = Consignments.objects(id_elemento=id_elemento).first()
        if consignaciones is None:
            return dict(success=False, msg="No existen consignaciones para este elemento. "
                                           "El elemento no existe"), 404
        success, consignacion = consignaciones.search_consignment_by_id(id_to_search=id_consignacion)
        if not success:
            return dict(success=False, msg="No existe consignación para este elemento"), 404
        consignacion.create_folder()
        file = args['file']
        filename = file.filename
        stream_file = file.stream.read()
        destination = os.path.join(init.CONS_REPO, id_consignacion, filename)
        with open(destination, 'wb') as f:
            f.write(stream_file)
        consignaciones.save()
        return dict(success=True, msg="Documento cargado exitosamente")


@ns.route('/consignaciones/<string:formato>/<string:ini_date>/<string:end_date>')
class ConsignacionDateAPI(Resource):
    def get(self, formato=None, ini_date: str = "yyyy-mm-dd hh:mm:ss", end_date: str = "yyyy-mm-dd hh:mm:ss"):
        """ Obtener las consignaciones existentes en las fechas consultadas
            formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
            formatos disponibles: [json, excel]
        """
        success, ini_date, end_date, msg = check_range_yyyy_mm_dd_hh_mm_ss(ini_date, end_date)
        if not success:
            return dict(success=False, msg=msg), 400

        permitido = ["excel", "json"]
        if not formato in ["excel", "json"]:
            msg = f"No se puede presentar el reporte en el formato {formato}, considere las opciones: {permitido}"
            return dict(success=False, msg=msg), 400

        #   D*****[***************H
        contiene_ini = Q(desde__lte=ini_date) & Q(hasta__gte=ini_date)
        #   D*****]***************H
        contiene_end = Q(desde__lte=end_date) & Q(hasta__gte=end_date)
        #   [     D********H    ]
        contenido_en = Q(desde__gte=ini_date) & Q(hasta__lte=end_date)
        time_query = contiene_ini | contiene_end | contenido_en
        consignaciones = Consignments.objects.filter(time_query)

        if len(consignaciones) == 0:
            return dict(success=False, msg=f"No se encontraron consignaciones para [{ini_date} @ {end_date}]"), 404
        consignment_result = list()
        for c_consignacion in consignaciones:
            consignment_dicts = c_consignacion.consignments_in_time_range_w_element(ini_date, end_date)
            consignment_result += consignment_dicts
        if len(consignment_result) == 0:
            return dict(success=False, msg=f"No se encontraron consignaciones para [{ini_date} @ {end_date}]"), 404

        if formato == 'json':
            return dict(success=True, consignaciones=consignment_result), 200

        ini_date_str, end_date_str = ini_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
        file_name = f"Consignaciones_{ini_date_str}@{end_date_str}.xlsx"
        path = os.path.join(init.TEMP_REPO, file_name)
        with pd.ExcelWriter(path) as writer:
            df_consignment = pd.DataFrame(consignment_result)
            df_consignment.to_excel(writer, sheet_name="consignaciones")
        if os.path.exists(path):
            resp = send_from_directory(os.path.dirname(path), file_name, as_attachment=True)
            return set_max_age_to_response(resp, 30)
        return dict(success=False, consignaciones=consignment_result), 404
