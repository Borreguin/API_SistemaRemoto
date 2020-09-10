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
# importando configuraciones iniciales
from api.services.restplus_config import api
from api.services.restplus_config import default_error_handler
from api.services.sRemoto import serializers as srl
# importando el motor de cálculos:
from motor.master_scripts.eng_sRmaster import *
from flask import request
# importando clases para leer desde MongoDB
from dto.mongo_engine_handler.ProcessingState import TemporalProcessingStateReport

ser_from = srl.sRemotoSerializers(api)
api = ser_from.add_serializers()

# configurando logger y el servicio web
log = init.LogDefaultConfig("ws_sRemoto.log").logger
ns = api.namespace('disp-sRemoto', description='Relativas a Sistema Remoto')


@api.errorhandler(Exception)
@ns.route('/disponibilidad/<string:ini_date>/<string:end_date>')
class Disponibilidad(Resource):

    @staticmethod
    def put(ini_date: str = "yyyy-mm-dd", end_date: str = "yyyy-mm-dd"):
        """ Calcula/sobre-escribe la disponibilidad de los nodos que se encuentren activos en base de datos
            Si ya existe reportes asociados a los nodos, estos son <b>recalculados</b>
            Este proceso puede tomar para su ejecución al menos <b>20 minutos</b>
            Fecha inicial formato:  <b>yyyy-mm-dd</b>
            Fecha final formato:    <b>yyyy-mm-dd</b>
        """
        try:
            success1, ini_date = u.check_date_yyyy_mm_dd(ini_date)
            success2, end_date = u.check_date_yyyy_mm_dd(end_date)
            if not success1 or not success2:
                msg = "No se puede convertir: " + (ini_date if not success1 else end_date)
                return dict(success=False, msg=msg), 400
            success1, result, msg1 = run_all_nodes(ini_date, end_date, save_in_db=True, force=True)
            if success1:
                success2, report, msg2 = run_summary(ini_date, end_date, save_in_db=True, force=True)
                if success2:
                    return dict(result=result, msg=msg1, report=report.to_dict()), 200
                else:
                    return dict(success=False, msg=msg2), 400
            elif not success1:
                return dict(success=False, msg=result), 400

        except Exception as e:
            return default_error_handler(e)

    @staticmethod
    def post(ini_date: str = "yyyy-mm-dd", end_date: str = "yyyy-mm-dd"):
        """ Calcula si no existe la disponibilidad de los nodos que se encuentren activos en base de datos
            Si ya existe reportes asociados a los nodos, estos <b>no son recalculados</b>
            Este proceso puede tomar para su ejecución al menos <b>20 minutos</b>
            Fecha inicial formato:  <b>yyyy-mm-dd</b>
            Fecha final formato:    <b>yyyy-mm-dd</b>
        """
        try:
            success1, ini_date = u.check_date_yyyy_mm_dd(ini_date)
            success2, end_date = u.check_date_yyyy_mm_dd(end_date)
            if not success1 or not success2:
                msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
                return dict(success=False, msg=msg), 400
            success1, result, msg1 = run_all_nodes(ini_date, end_date, save_in_db=True)
            not_calculated = [True for k in result.keys() if "No ha sido calculado" in result[k]]
            if all(not_calculated) and len(not_calculated) > 0:
                return dict(success=False, msg="No ha sido calculado, ya existe en base de datos")
            if success1:
                success2, report, msg2 = run_summary(ini_date, end_date, save_in_db=True, force=True)
                if success2:
                    return dict(result=result, msg=msg1, report=report.to_dict()), 200
                else:
                    return dict(success=False, msg=msg2), 400
            elif not success1:
                return dict(success=False, msg=result), 400

        except Exception as e:
            return default_error_handler(e)

    @staticmethod
    def get(ini_date: str = "yyyy-mm-dd", end_date: str = "yyyy-mm-dd"):
        """ Entrega el cálculo realizado por acciones POST/PUT
            Si el cálculo no existe entonces <b>código 404</b>
            Fecha inicial formato:  <b>yyyy-mm-dd</b>
            Fecha final formato:    <b>yyyy-mm-dd</b>
        """
        try:
            success1, ini_date = u.check_date_yyyy_mm_dd(ini_date)
            success2, end_date = u.check_date_yyyy_mm_dd(end_date)
            if not success1 or not success2:
                msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
                return dict(success=False, msg=msg), 400
            final_report_v = SRFinalReport(fecha_inicio=ini_date, fecha_final=end_date)
            final_report = SRFinalReport.objects(id_report=final_report_v.id_report).first()
            if final_report is None:
                return dict(success=False, msg="No existe reporte asociado"), 404
            return final_report.to_dict(), 200

        except Exception as e:
            return default_error_handler(e)


@api.errorhandler(Exception)
@ns.route('/disponibilidad/nodo/<string:ini_date>/<string:end_date>')
class DisponibilidadNodo(Resource):
    @staticmethod
    @api.expect(ser_from.nodo)
    def put(ini_date: str = "yyyy-mm-dd", end_date: str = "yyyy-mm-dd"):
        """ Calcula/sobre-escribe la disponibilidad de un nodo especificado por tipo y nombre
            Si ya existe reporte asociados al nodo, este es <b>recalculado</b>
            Fecha inicial formato:  <b>yyyy-mm-dd</b>
            Fecha final formato:    <b>yyyy-mm-dd</b>
        """
        try:
            success1, ini_date = u.check_date_yyyy_mm_dd(ini_date)
            success2, end_date = u.check_date_yyyy_mm_dd(end_date)
            if not success1 or not success2:
                msg = "No se puede convertir: " + (ini_date if not success1 else end_date)
                return dict(success=False, msg=msg), 400

            node_list_name = [request.json["nombre"]]
            success1, result, msg1 = run_node_list(node_list_name, ini_date, end_date, save_in_db=True, force=True)
            if success1:
                success2, report, msg2 = run_summary(ini_date, end_date, save_in_db=True, force=True)
                if success2:
                    return dict(result=result, msg=msg1, report=report.to_dict()), 200
                else:
                    return dict(success=False, msg=msg2), 409
            elif not success1:
                return dict(success=False, msg=result), 409
        except Exception as e:
            return default_error_handler(e)


@api.errorhandler(Exception)
@ns.route('/disponibilidad/nodos/<string:ini_date>/<string:end_date>')
class DisponibilidadNodos(Resource):
    @staticmethod
    @api.expect(ser_from.nodes)
    def put(ini_date: str = "yyyy-mm-dd", end_date: str = "yyyy-mm-dd"):
        """ Calcula/sobre-escribe la disponibilidad de los nodos especificados en la lista
            Si ya existe reportes asociados a los nodos, estos son <b>recalculados</b>
            Fecha inicial formato:  <b>yyyy-mm-dd</b>
            Fecha final formato:    <b>yyyy-mm-dd</b>
        """
        try:
            success1, ini_date = u.check_date_yyyy_mm_dd(ini_date)
            success2, end_date = u.check_date_yyyy_mm_dd(end_date)
            if not success1 or not success2:
                msg = "No se puede convertir: " + (ini_date if not success1 else end_date)
                return dict(success=False, msg=msg), 400

            node_list_name = request.json["nodos"]
            success1, result, msg1 = run_node_list(node_list_name, ini_date, end_date, save_in_db=True, force=True)
            if success1:
                success2, report, msg2 = run_summary(ini_date, end_date, save_in_db=True, force=True)
                if success2:
                    return dict(result=result, msg=msg1, report=report.to_dict()), 200
                else:
                    return dict(success=False, msg=msg2), 409
            elif not success1:
                return dict(success=False, msg=result), 409
        except Exception as e:
            return default_error_handler(e)

    @staticmethod
    @api.expect(ser_from.nodes)
    def post(ini_date: str = "yyyy-mm-dd", end_date: str = "yyyy-mm-dd"):
        """ Calcula si no existe la disponibilidad de los nodos especificados en la lista
            Si ya existe reportes asociados a los nodos, estos <b>no son recalculados</b>
            Fecha inicial formato:  <b>yyyy-mm-dd</b>
            Fecha final formato:    <b>yyyy-mm-dd</b>
        """
        try:
            success1, ini_date = u.check_date_yyyy_mm_dd(ini_date)
            success2, end_date = u.check_date_yyyy_mm_dd(end_date)
            if not success1 or not success2:
                msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
                return dict(success=False, msg=msg), 400
            node_list_name = request.json["nodos"]
            success1, result, msg1 = run_node_list(node_list_name, ini_date, end_date, save_in_db=True, force=False)
            not_calculated = [True for k in result.keys() if "No ha sido calculado" in result[k]]
            if all(not_calculated) and len(not_calculated) > 0:
                return dict(success=False, msg="No ha sido calculado, ya existe en base de datos")
            if success1:
                success2, report, msg2 = run_summary(ini_date, end_date, save_in_db=True, force=True)
                if success2:
                    return dict(result=result, msg=msg1, report=report.to_dict()), 200
                else:
                    return dict(success=False, msg=msg2), 409
            elif not success1:
                return dict(success=False, msg=result), 409

        except Exception as e:
            return default_error_handler(e)

    @staticmethod
    @api.expect(ser_from.nodes)
    def delete(ini_date: str = "yyyy-mm-dd", end_date: str = "yyyy-mm-dd"):
        """ Elimina si existe la disponibilidad de los nodos especificados en la lista
            Fecha inicial formato:  <b>yyyy-mm-dd</b>
            Fecha final formato:    <b>yyyy-mm-dd</b>
        """
        try:
            success1, ini_date = u.check_date_yyyy_mm_dd(ini_date)
            success2, end_date = u.check_date_yyyy_mm_dd(end_date)
            if not success1 or not success2:
                msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
                return dict(success=False, msg=msg), 400
            node_list_name = request.json["nodos"]
            not_found = list()
            deleted = list()
            for node in node_list_name:
                sR_node = SRNode.objects(nombre=node).first()
                if sR_node is None:
                    not_found.append(node)
                    continue
                # delete details report fo this node
                report_v = SRNodeDetails(nodo=sR_node, nombre=sR_node.nombre, tipo=sR_node.tipo,
                                         fecha_inicio=ini_date, fecha_final=end_date)
                report = SRNodeDetails.objects(id_report=report_v.id_report).first()
                if report is None:
                    not_found.append(node)
                    continue
                report.delete()
                deleted.append(node)
                # delete status report if exists
                status_report = TemporalProcessingStateReport.objects(id_report=report_v.id_report).first()
                if status_report is not None:
                    status_report.delete()

            if len(deleted) == 0:
                return dict(success=False, deleted=deleted, not_found=not_found), 404
            success, _, msg = run_summary(ini_date, end_date, save_in_db=True, force=True)
            if not success:
                return dict(success=False, deleted=[], not_found=not_found), 400
            return dict(success=True, deleted=deleted, not_found=not_found), 200

        except Exception as e:
            return default_error_handler(e)


@api.errorhandler(Exception)
@ns.route('/disponibilidad/<string:tipo>/<string:nombre>/<string:ini_date>/<string:end_date>')
class DisponibilidadNodo(Resource):
    @staticmethod
    def get(tipo="tipo de nodo", nombre="nombre del nodo", ini_date: str = "yyyy-mm-dd", end_date: str = "yyyy-mm-dd"):
        """ Obtiene el reporte de disponibilidad de los nodos especificados en la lista
            Si el reporte no existe, entonces su valor será 404
            Fecha inicial formato:  <b>yyyy-mm-dd</b>
            Fecha final formato:    <b>yyyy-mm-dd</b>
        """
        try:
            success1, ini_date = u.check_date_yyyy_mm_dd(ini_date)
            success2, end_date = u.check_date_yyyy_mm_dd(end_date)
            if not success1 or not success2:
                msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
                return dict(success=False, msg=msg), 400
            v_report = SRNodeDetails(tipo=tipo, nombre=nombre, fecha_inicio=ini_date, fecha_final=end_date)
            report = SRNodeDetails.objects(id_report=v_report.id_report).first()
            if report is None:
                return dict(success=False, msg="No existe el cálculo para este nodo en la fecha indicada"), 404
            else:
                return report.to_dict(), 200

        except Exception as e:
            return default_error_handler(e)

    @staticmethod
    def delete(tipo="tipo de nodo", nombre="nombre del nodo", ini_date: str = "yyyy-mm-dd", end_date: str = "yyyy-mm-dd"):
        """ Elimina el reporte de disponibilidad del nodo especificado
            Si el reporte no existe, entonces código 404
            Fecha inicial formato:  <b>yyyy-mm-dd</b>
            Fecha final formato:    <b>yyyy-mm-dd</b>
        """
        try:
            success1, ini_date = u.check_date_yyyy_mm_dd(ini_date)
            success2, end_date = u.check_date_yyyy_mm_dd(end_date)
            if not success1 or not success2:
                msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
                return dict(success=False, msg=msg), 400
            v_report = SRNodeDetails(tipo=tipo, nombre=nombre, fecha_inicio=ini_date, fecha_final=end_date)
            report = SRNodeDetails.objects(id_report=v_report.id_report).first()
            if report is None:
                return dict(success=False, msg="No existe el cálculo para este nodo en la fecha indicada"), 404
            else:
                report.delete()
                success, final_report, msg = run_summary(ini_date, end_date, save_in_db=True, force=True)
                if not success:
                    return dict(success=False, msg="No se puede calcular el reporte final"), 404
                return dict(success=True, report=final_report.to_dict(),
                            msg=f"El reporte del nodo {nombre} ha sido eliminado, "
                                f"y el reporte final ha sido re-calculado"), 200

        except Exception as e:
            return default_error_handler(e)


@api.errorhandler(Exception)
@ns.route('/estado/disponibilidad/<string:ini_date>/<string:end_date>')
class DisponibilidadStatusNodo(Resource):
    @staticmethod
    def get(ini_date: str = "yyyy-mm-dd", end_date: str = "yyyy-mm-dd"):
        """ Obtiene el estado del cálculo de los reportes de disponibilidad existentes en el periodo especificado
            Nota: Este servicio es válido solamente al momento de consultar el estado de un cálculo en proceso
            en otro contexto, este servicio no garantiza un comportamiento correcto
            Fecha inicial formato:  <b>yyyy-mm-dd</b>
            Fecha final formato:    <b>yyyy-mm-dd</b>
        """
        try:
            success1, ini_date = u.check_date_yyyy_mm_dd(ini_date)
            success2, end_date = u.check_date_yyyy_mm_dd(end_date)
            if not success1 or not success2:
                msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
                return dict(success=False, msg=msg), 400
            # check the existing nodes:
            all_nodes = SRNode.objects()
            all_nodes = [n for n in all_nodes if n.activado]
            if len(all_nodes) == 0:
                msg = f"No se ecnuentran nodos que procesar"
                return dict(success=False, msg=msg), 404

            # scan reports within this date range:
            to_send = list()
            for node in all_nodes:
                v_report = SRNodeDetails(nodo=node, nombre=node.nombre, tipo=node.tipo,
                                         fecha_inicio=ini_date, fecha_final=end_date)
                tmp_report = TemporalProcessingStateReport.objects(id_report=v_report.id_report).first()
                if tmp_report is None:
                    continue
                to_send.append(tmp_report.to_summary())
            return dict(success=True, status=to_send), 200

        except Exception as e:
            return default_error_handler(e)