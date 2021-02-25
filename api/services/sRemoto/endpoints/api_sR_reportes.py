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
from flask import request, send_from_directory
# importando clases para leer desde MongoDB
from dto.mongo_engine_handler.ProcessingState import TemporalProcessingStateReport

ser_from = srl.sRemotoSerializers(api)
api = ser_from.add_serializers()

# configurando logger y el servicio web
log = init.LogDefaultConfig("ws_sRemoto.log").logger
ns = api.namespace('sRemoto', description='Relativas a reportes personalizados de Sistema Remoto')


@ns.route('/disponibilidad/excel/<string:ini_date>/<string:end_date>')
class DisponibilidadExcel(Resource):

    @staticmethod
    def get(ini_date: str = "yyyy-mm-dd", end_date: str = "yyyy-mm-dd"):
        """ Entrega el cálculo en formato Excel realizado por acciones POST/PUT
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
            # Creating an Excel file:
            ini_date_str, end_date_str = ini_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
            file_name = f"R_{ini_date_str}.xlsx"
            path = os.path.join(init.TEMP_PATH, file_name)
            success, df_summary, df_details, df_novedades = final_report.to_dataframe()
            if not success:
                return dict(success=False, msg="Existe problemas al convertir en excel el reporte"), 409
            with pd.ExcelWriter(path) as writer:
                df_summary.to_excel(writer, sheet_name="Resumen")
                df_details.to_excel(writer, sheet_name="Detalles")
                df_novedades.to_excel(writer, sheet_name="Novedades")
            if os.path.exists(path):
                return send_from_directory(os.path.dirname(path), file_name, as_attachment=False)

        except Exception as e:
            return default_error_handler(e)


@ns.route('/disponibilidad/json/<string:ini_date>/<string:end_date>')
class DisponibilidadJSON(Resource):

    @staticmethod
    def get(ini_date: str = "yyyy-mm-dd", end_date: str = "yyyy-mm-dd"):
        """ Entrega el cálculo en formato JSON realizado por acciones POST/PUT
            Si el cálculo no existe entonces <b>código 404</b>
            Fecha inicial formato:  <b>yyyy-mm-dd</b>
            Fecha final formato:    <b>yyyy-mm-dd</b>
        """
        try:
            success1, ini_date = u.check_date_yyyy_mm_dd(ini_date)
            success2, end_date = u.check_date_yyyy_mm_dd(end_date)
            if not success1 or not success2:
                msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
                return dict(success=False, report=None, msg=msg), 400
            # generando reporte virtual para obtener el id:
            final_report_v = SRFinalReport(fecha_inicio=ini_date, fecha_final=end_date)
            # buscando reporte en base de datos a través del id: final_report_v.id_report
            final_report = SRFinalReport.objects(id_report=final_report_v.id_report).first()
            if final_report is None:
                return dict(success=False, report=None, msg="No existe reporte asociado"), 404
            # Creating an Excel file:
            success, df_summary, df_details, df_novedades = final_report.to_dataframe()
            if not success:
                return dict(success=False, report=None, msg="Existe problemas al adquirir el reporte"), 409
            result_dict = dict()
            result_dict["Resumen"] = df_summary.to_dict(orient='records')
            result_dict["Detalles"] = df_details.to_dict(orient='records')
            result_dict["Novedades"] = df_novedades.to_dict(orient='records')
            return dict(success=True, report=result_dict, msg="Reporte encontrado")

        except Exception as e:
            return default_error_handler(e)


@ns.route('/indisponibilidad/tags/<string:formato>/<string:ini_date>/<string:end_date>')
@ns.route('/indisponibilidad/tags/<string:formato>/<string:ini_date>/<string:end_date>/<string:umbral>')
@ns.route('/indisponibilidad/tags/<string:formato>/<string:ini_date>/<string:end_date>/<string:umbral>/<string:rand_key>')
class IndisponibilidadTAGSs(Resource):

    @staticmethod
    def get(formato, ini_date: str = "yyyy-mm-dd", end_date: str = "yyyy-mm-dd", umbral=None, rand_key=None):
        """ Entrega el listado de tags cuya indisponibilidad sea mayor igual al umbral (por defecto 0)
            Si el cálculo no existe entonces <b>código 404</b>
            Formato:                excel, json
            Fecha inicial formato:  <b>yyyy-mm-dd</b>
            Fecha final formato:    <b>yyyy-mm-dd</b>
            Umbral:                 <b>float</b>
            Rand_key:               <b>cualquier valor, permite actualizar el reporte</b>
        """
        try:
            success1, ini_date = u.check_date_yyyy_mm_dd(ini_date)
            success2, end_date = u.check_date_yyyy_mm_dd(end_date)
            if not success1 or not success2:
                msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
                return dict(success=False, msg=msg), 400
            # definición del umbral
            if umbral is None:
                umbral = 0
            else:
                umbral = float(umbral)

            # formato permitido:
            permitido = ["excel", "json"]
            if not formato in ["excel", "json"]:
                msg = f"No se puede presentar el reporte en el formato {formato}, considere las opciones: {permitido}"
                return dict(success=False, msg=msg), 400

            # Obtener el reporte final con los detalles de cada reporte por nodo
            final_report_virtual = SRFinalReport(fecha_inicio=ini_date, fecha_final=end_date)
            final_report_db = SRFinalReport.objects(id_report=final_report_virtual.id_report).first()

            # variable para guardar el listado de tags con su respectiva indisponibilidad
            df_tag = pd.DataFrame(columns=[lb_empresa, lb_unidad_negocio, lb_utr_id, lb_utr,
                                           lb_tag_name, lb_indisponible_minutos])

            for reporte_nodo_resumen in final_report_db.reportes_nodos:
                reporte_nodo_db = SRNodeDetails.objects(id_report=reporte_nodo_resumen.id_report).first()
                row = {lb_empresa: reporte_nodo_db.nombre}
                for reporte_entidad in reporte_nodo_db.reportes_entidades:
                    row[lb_unidad_negocio] = reporte_entidad.entidad_nombre
                    for reporte_utr in reporte_entidad.reportes_utrs:
                        row[lb_utr_id] = reporte_utr.id_utr
                        row[lb_utr] = reporte_utr.utr_nombre
                        indisponibilidad_detalle = [tag for tag in reporte_utr.indisponibilidad_detalle
                                                    if tag.indisponible_minutos >= umbral]
                        for tag in indisponibilidad_detalle:
                            row.update(tag.to_dict())
                            df_tag = df_tag.append(row, ignore_index=True)

            if formato == "json":
                resp = df_tag.to_dict(orient="records")
                return dict(success=True, reporte=resp)

            if formato == "excel":
                # nombre del archivo
                file_name = f"IndispTags{ini_date.day}-{ini_date.month}" \
                            f"@{end_date.day}-{end_date.month}-{end_date.year}.xlsx"
                path = os.path.join(init.TEMP_PATH, file_name)
                # crear en el directorio temporal para envío del archivo
                with pd.ExcelWriter(path) as writer:
                    df_tag.to_excel(writer, sheet_name="Detalles")
                if os.path.exists(path):
                    return send_from_directory(os.path.dirname(path), file_name, as_attachment=True)

        except Exception as e:
            return default_error_handler(e)


"""
@ns.route('/disponibilidad/diaria')
class DisponibilidadDiaria(Resource):

    @staticmethod
    def get():
         Entrega el cálculo de disponibilidad del último día en formato JSON
            Si el cálculo no existe entonces <b>código 404</b>
        
        try:
            now = dt.datetime.now()
            now = dt.datetime(2020, 7, 2)
            end_date = dt.datetime(now.year, now.month, now.day)
            ini_date = end_date - dt.timedelta(days=1)
            final_report_v = SRFinalReport(fecha_inicio=ini_date, fecha_final=end_date)
            final_report = SRFinalReport.objects(id_report=final_report_v.id_report).first()
            if final_report is None:
                return dict(success=False, report=None, msg="No existe reporte asociado"), 404
            success, df_summary, df_details, df_novedades = final_report.to_dataframe()
            if not success:
                return dict(success=False, msg="Existe problemas al convertir en excel el reporte"), 409
            return dict(success=True)

        except Exception as e:
            return default_error_handler(e)

"""


