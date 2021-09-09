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

from flask import request
from flask_restplus import Resource
from flask_app.api.services.Consignaciones import parsers, serializers as srl
from flask_app.api.services.restplus_config import api
from flask_app.api.services.restplus_config import default_error_handler
# importando clases para leer desde MongoDB
# importando configuraciones iniciales
from flask_app.dto.mongo_engine_handler.Consignment import Consignments, Consignment
from flask_app.my_lib import utils as u
from flask_app.settings import initial_settings as init

# configurando el servicio web
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
        try:
            success1, ini_date = u.check_date_yyyy_mm_dd_hh_mm_ss(ini_date)
            success2, end_date = u.check_date_yyyy_mm_dd_hh_mm_ss(end_date)
            if not success1 or not success2:
                msg = "No se puede convertir: " + (ini_date if not success1 else end_date)
                return dict(success=False, msg=msg), 400

            consignacion = Consignments.objects(id_elemento=id_elemento).first()
            if consignacion is None:
                return dict(success=False, msg="No existen consignaciones asociadas a este elemento"), 404
            consignaciones = consignacion.consignments_in_time_range(ini_date, end_date)
            if len(consignaciones) == 0:
                return dict(success=False, msg="No existen consignaciones en el periodo especificado"), 404
            return dict(success=True, consignaciones=[c.to_dict() for c in consignaciones],
                        msg="Se han encontrado consignaciones asociadas"), 200

        except Exception as e:
            return default_error_handler(e)

    @api.expect(ser_from.detalle_consignacion)
    def post(self, id_elemento: str = "id_elemento", ini_date: str = "yyyy-mm-dd hh:mm:ss",
             end_date: str = "yyyy-mm-dd hh:mm:ss"):
        """ Consignar un elemento asociadas a: "id_elemento"
            <b>id_elemento</b> corresponde al elemento a consignar
             formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
        """
        try:
            success1, ini_date = u.check_date_yyyy_mm_dd_hh_mm_ss(ini_date)
            success2, end_date = u.check_date_yyyy_mm_dd_hh_mm_ss(end_date)
            if not success1 or not success2:
                msg = "No se puede convertir: " + (ini_date if not success1 else end_date)
                return dict(success=False, msg=msg), 400
            if ini_date >= end_date:
                msg = "El rango de fechas es incorrecto. Revise que la fecha inicial sea anterior a fecha final"
                return dict(success=False, msg=msg), 400
            detalle = dict(request.json)
            consignaciones = Consignments.objects(id_elemento=id_elemento).first()
            if consignaciones is None:
                new_consignments = Consignments(id_elemento=id_elemento)
                new_consignments.save()
                consignaciones = new_consignments
            consignaciones.elemento = detalle.get("elemento", None)
            consignacion = Consignment(no_consignacion=detalle["no_consignacion"], fecha_inicio=ini_date,
                                       fecha_final=end_date, detalle=detalle["detalle"])
            # ingresando consignación y guardando si es exitoso:
            success, msg = consignaciones.insert_consignments(consignacion)
            if success:
                consignaciones.save()
                return dict(success=success, msg=msg)
            else:
                return dict(success=success, msg=msg)

        except Exception as e:
            return default_error_handler(e)


@ns.route('/consignacion/<string:id_elemento>/<string:id_consignacion>')
class ConsignacionDeleteEditAPI(Resource):

    def delete(self, id_elemento: str = "id_elemento", id_consignacion: str = "id_consignacion"):
        """ Elimina la consignación asociadas del elemento: "id_elemento" cuya idenficación es "id_consignacion"
            <b>id_elemento</b> corresponde al elemento consignado
            <b>id_consignacion</b> corresponde a la identificación de la consignación
        """
        try:
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

        except Exception as e:
            return default_error_handler(e)

    @api.expect(ser_from.consignacion)
    def put(self, id_elemento: str = "id_elemento", id_consignacion: str = "id_consignacion"):
        """ Edita la consignación asociada al elemento: "id_elemento", cuya idenficación es "id_consignacion"
            <b>id_elemento</b> corresponde al elemento consignado
            <b>id_consignacion</b> corresponde a la identificación de la consignación
            formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
        """
        try:
            detalle = request.json
            consignaciones = Consignments.objects(id_elemento=id_elemento).first()
            if consignaciones is None:
                return dict(success=False, msg="No existen consignaciones para este elemento. "
                                               "El elemento no existe"), 404

            # eliminando consignación por id
            consignacion = Consignment(**detalle)
            success, msg = consignaciones.edit_consignment_by_id(id_to_edit=id_consignacion, consignment=consignacion)
            if success:
                consignaciones.save()
                return dict(success=success, msg=msg), 200
            else:
                return dict(success=success, msg=msg), 404

        except Exception as e:
            return default_error_handler(e)

    @api.expect(parsers.consignacion_upload)
    def post(self, id_elemento: str = "id_elemento", id_consignacion: str = "id_consignacion"):
        """ Carga un archivo a la consignación asociada al elemento: "id_elemento", cuya idenficación es "id_consignacion"
                    <b>id_elemento</b> corresponde al elemento consignado
                    <b>id_consignacion</b> corresponde a la identificación de la consignación
                    formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
        """
        try:
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
        except Exception as e:
            return default_error_handler(e)
