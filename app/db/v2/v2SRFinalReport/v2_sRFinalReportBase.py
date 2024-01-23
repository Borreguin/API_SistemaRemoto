from __future__ import annotations
import re

import pandas as pd
from mongoengine import StringField, DateTimeField, IntField, FloatField, ListField, EmbeddedDocumentField, \
    ReferenceField, DictField, Document

from app.common import report_log
import datetime as dt

from app.common.util import get_time_in_minutes
from app.db.constants import SR_REPORTE_SISTEMA_REMOTO, V2_SR_FINAL_REPORT_LABEL
from app.db.v2.entities.v2_sRNode import V2SRNode
from app.db.v2.v2SRFinalReport.V2SRNodeSummaryReport import V2SRNodeSummaryReport
from app.db.v2.v2SRFinalReport.constants import *
from app.db.v2.v2SRNodeReport.V2SRNodeDetailsBase import V2SRNodeDetailsBase
from app.db.v2.v2SRNodeReport.V2SRNodeDetailsPermanent import V2SRNodeDetailsPermanent
from app.db.v2.v2SRNodeReport.V2SRNodeDetailsTemporal import V2SRNodeDetailsTemporal
from app.db.v2.v2SRNodeReport.report_util import get_final_report_id
from app.utils import utils as u
from app.db.v1.SRNodeReport.sRNodeReportPermanente import SRNodeDetailsPermanente
from multiprocessing.pool import ThreadPool
import queue


class V2SRFinalReportBase(Document):
    id_report = StringField(required=True, unique=True)
    tipo = StringField(required=True, default=SR_REPORTE_SISTEMA_REMOTO)
    fecha_inicio = DateTimeField(required=True)
    fecha_final = DateTimeField(required=True)
    periodo_evaluacion_minutos = IntField(required=True)
    # el valor -1 es aceptado en el caso de que la disponibilidad no este definida
    disponibilidad_promedio_ponderada_porcentage = FloatField(required=True, min_value=-1, max_value=100, default=-1)
    disponibilidad_promedio_porcentage = FloatField(required=True, min_value=-1, max_value=100, default=-1)
    reportes_nodos = ListField(EmbeddedDocumentField(V2SRNodeSummaryReport))
    reportes_nodos_detalle = ListField(ReferenceField(V2SRNodeDetailsBase, dbref=True), required=False)
    tiempo_calculo_segundos = FloatField(default=0)
    procesamiento = DictField(default=dict(numero_tags=0,numero_bahias_procesadas=0, numero_instalaciones_procesadas=0,
                                           numero_entidades_procesadas=0, numero_nodos_procesados=0))
    novedades = DictField(default=dict(tags_fallidas=0, instalaciones_fallidas=0,
                                       entidades_fallidas=0, nodos_fallidos=0, detalle={}), required=False)
    documento = StringField(default=V2_SR_FINAL_REPORT_LABEL, required=False)
    actualizado = DateTimeField(default=dt.datetime.now())
    meta = {'allow_inheritance': True, 'abstract': True}
    # attributos utilizados para calculos internos:
    nodes_info = None
    instalaciones_dict = None

    def __init__(self, *args, **values):
        super().__init__(*args, **values)
        self.installation_dict = dict()
        if self.id_report is None:
            self.id_report = get_final_report_id(self.tipo, self.fecha_inicio, self.fecha_final)
        self.periodo_evaluacion_minutos = get_time_in_minutes(self.fecha_inicio, self.fecha_final)
        if self.actualizado is None:
            self.actualizado = dt.datetime.now()

    def novedades_as_dict(self):
        detalle = self.novedades.pop("detalle", None)
        ind_dict = self.novedades
        detalle["todos los nodos"] = ind_dict
        lst_final = []
        if isinstance(detalle, dict):
            results = detalle.pop("results", {})
            logs = detalle.pop("log", {})
            for key in detalle:
                ind_dict = detalle[key]
                if ind_dict is None or isinstance(ind_dict, list):
                    continue
                ind_dict["item"] = key
                if key in results.keys():
                    ind_dict["result"] = results[key]
                log_lst = list()
                for log in logs:
                    if key in log:
                        log_lst.append(log)
                ind_dict["log"] = log_lst
                lst_final.append(ind_dict)
        return lst_final

    def append_each_node_detail(self, label, node_summary_report: V2SRNodeSummaryReport):
        self.procesamiento[label] = self.procesamiento.get(label,0) + node_summary_report.procesamiento.get(label, 0)

    def count_novedades(self, label, node_summary_report: V2SRNodeSummaryReport):
        self.novedades[label] = self.novedades.get(label, 0) + node_summary_report.novedades.get(label, 0)
        if self.novedades[label] > 0:
            name = re.sub(r"[^a-zA-Z0-9]+", '_', node_summary_report.nombre)
            self.novedades[lb_detalle][name] = self.novedades[lb_detalle].get(name, {})
            self.novedades[lb_detalle][name][label] = node_summary_report.novedades.get(label, 0)

    def append_node_detail_report(self, d_report: V2SRNodeDetailsTemporal| V2SRNodeDetailsPermanent):
        summary_report = V2SRNodeSummaryReport().set_values_from_detail_report(d_report)
        self.append_node_summary_report(summary_report)

    def append_node_summary_report(self, node_summary_report: V2SRNodeSummaryReport):
        self.reportes_nodos.append(node_summary_report)
        to_work_with = [lb_numero_tags, lb_numero_tags_procesadas, lb_numero_bahias_procesadas, lb_numero_instalaciones_procesadas, lb_numero_entidades_procesadas]
        [self.append_each_node_detail(label, node_summary_report) for label in to_work_with]

        novedades_to_work_with = [lb_tags_fallidas, lb_bahias_fallidas, lb_instalaciones_fallidas, lb_entidades_fallidas]
        [self.count_novedades(label, node_summary_report) for label in novedades_to_work_with]

    def calculate_percentage_average(self):
        if len(self.reportes_nodos) == 0:
            return 0, 0

        # this takes the weighted average of the nodes and make the average of the nodes
        self.disponibilidad_promedio_porcentage, n_reports, n_tags = 0, 0, 0
        for report in self.reportes_nodos:
            if report.disponibilidad_promedio_ponderada_porcentage > 0:
                self.disponibilidad_promedio_porcentage += report.disponibilidad_promedio_ponderada_porcentage
                n_reports += 1
                n_tags += report.procesamiento[lb_numero_tags]
        if n_reports > 0:
            self.disponibilidad_promedio_porcentage = self.disponibilidad_promedio_porcentage / n_reports
        return n_reports, n_tags

    def calculate_percentage_weighted_average(self, n_report, n_tags):
        if n_report > 0 and n_tags > 0:
            self.disponibilidad_promedio_ponderada_porcentage = 0
            for report in self.reportes_nodos:
                if report.disponibilidad_promedio_ponderada_porcentage > 0:
                    self.disponibilidad_promedio_ponderada_porcentage += \
                        report.disponibilidad_promedio_ponderada_porcentage * report.procesamiento[
                            lb_numero_tags] / n_tags
            if self.disponibilidad_promedio_ponderada_porcentage > 100:
                self.disponibilidad_promedio_ponderada_porcentage = 100

    def calculate(self):
        # cálculo de la disponibilidad promedio:
        n_report, n_tags = self.calculate_percentage_average()
        # cálculo de la disponibilidad promedio ponderada:
        self.calculate_percentage_weighted_average(n_report, n_tags)
        reportes_nodos = sorted(self.reportes_nodos, key=lambda k: k["disponibilidad_promedio_ponderada_porcentage"])
        self.procesamiento[lb_numero_nodos_procesados] = len(self.reportes_nodos)
        self.reportes_nodos = reportes_nodos

    def to_dict(self):
        return dict(id_report=self.id_report, tipo=self.tipo, fecha_inicio=str(self.fecha_inicio),
                    fecha_final=str(self.fecha_final), periodo_evaluacion_minutos=self.periodo_evaluacion_minutos,
                    disponibilidad_promedio_ponderada_porcentage=self.disponibilidad_promedio_ponderada_porcentage,
                    disponibilidad_promedio_porcentage=self.disponibilidad_promedio_porcentage,
                    reportes_nodos=[r.to_dict() for r in self.reportes_nodos], procesamiento=self.procesamiento,
                    novedades=self.novedades, actualizado=str(self.actualizado),
                    tiempo_calculo_segundos=self.tiempo_calculo_segundos, documento=self.documento)

    def to_table(self):
        resp = dict(id_report=self.id_report, tipo=self.tipo, fecha_inicio=str(self.fecha_inicio),
                    fecha_final=str(self.fecha_final), periodo_evaluacion_minutos=self.periodo_evaluacion_minutos,
                    disponibilidad_promedio_ponderada_porcentage=self.disponibilidad_promedio_ponderada_porcentage,
                    disponibilidad_promedio_porcentage=self.disponibilidad_promedio_porcentage,
                    actualizado=str(self.actualizado),
                    tiempo_calculo_segundos=self.tiempo_calculo_segundos, documento=self.documento)
        resp.update(self.procesamiento)
        return resp

    def load_nodes_info(self):
        try:
            self.nodes_info = list()
            # TODO: RS check if needed to write a v2 version
            nodes_info = V2SRNode.objects(document="SRNode").as_pymongo()
            self.nodes_info = [n for n in nodes_info]
        except Exception as e:
            report_log.error(f"{str(e)}")
            self.nodes_info = []
        return self

    def create_installations_dict(self):
        self.instalaciones_dict = dict()
        for node_info in self.nodes_info:
            for entidad in node_info[lb_entidades]:
                # TODO: RS this is going to crash
                for instalacion in entidad[lb_instalaciones]:
                    self.instalaciones_dict[instalacion[lb_instalacion_id]] = instalacion
        return self.instalaciones_dict

    # TODO: RS to check the complete function
    def process_this_node_detail_report(self, row:dict, detail_report):
        # report_log.info(f"Procesando reporte: {detail_report.nombre}")
        rows = list()
        for reporte_entidad in detail_report.reportes_entidades:
            # print(reporte_entidad.entidad_nombre)
            row[lb_dispo_ponderada_unidad] = reporte_entidad.disponibilidad_promedio_ponderada_porcentage / 100
            row[lb_unidad_negocio] = reporte_entidad.entidad_nombre
            for reporte_instalacion in reporte_entidad.reportes_instalaciones:
                row[lb_dispo_promedio_instalacion] = reporte_instalacion.disponibilidad_promedio_porcentage / 100
                row[lb_instalacion] = reporte_instalacion.instalacion_nombre
                row[lb_no_seniales] = reporte_instalacion.numero_tags
                row[lb_indisponible_minutos_promedio] = \
                    reporte_instalacion.indisponibilidad_acumulada_minutos / reporte_instalacion.numero_tags \
                        if reporte_instalacion.numero_tags > 0 else -1
                f_utr = self.installation_dict.get(reporte_instalacion.instalacion_id, None)
                row[lb_protocolo] = f_utr.get(lb_protocolo, None) if f_utr is not None else None
                row[lb_latitud] = f_utr.get(lb_latitud, None) if f_utr is not None else None
                row[lb_longitud] = f_utr.get(lb_longitud, None) if f_utr is not None else None
                # report_log.info(f"Finalizando reporte: {self.id_report}, {reporte_entidad.entidad_nombre}")
                rows.append(row.copy())
        return rows

    def to_dataframe(self, installation_dict: dict, q: queue.Queue = None):
        self.installation_dict = installation_dict
        try:
            # cola para recibir resultados
            report_log.info("Empezando la transformación a Dataframe")
            out_queue = queue.Queue()
            df_details = pd.DataFrame(columns=details_columns)
            summary = self.to_table()
            df_summary = pd.DataFrame(columns=list(summary.keys()))
            df_summary = df_summary.append(summary, ignore_index=True)
            df_novedades = pd.DataFrame(columns=["item", lb_tags_fallidas, lb_bahias_fallidas,
                                                 lb_instalaciones_fallidas, lb_entidades_fallidas,
                                                 lb_nodos_fallidos, "result", "log"], data=self.novedades_as_dict())
            df_novedades.set_index("item", inplace=True)
            row = {lb_fecha_ini: str(self.fecha_inicio), lb_fecha_fin: str(self.fecha_final)}
            # loading node info for each detail report
            n_threads = 0
            results = []
            n_pool = min(max(5, int(len(self.reportes_nodos)/5)), len(self.reportes_nodos))
            pool = ThreadPool(n_pool)
            report_log.info(f"Procesando la información de: {self.fecha_inicio}, {self.fecha_final}")
            for general_report in self.reportes_nodos:
                # recolectando reportes por cada nodo interno:
                if u.isTemporal(self.fecha_inicio, self.fecha_final):
                    detail_report = V2SRNodeDetailsTemporal.objects(id_report=general_report.id_report).first()
                else:
                    detail_report = SRNodeDetailsPermanente.objects(id_report=general_report.id_report).first()
                if detail_report is None:
                    report_log.warning(f"Este reporte no existe para este nodo: {general_report}")
                    continue
                # creating rows to process:
                row[lb_dispo_ponderada_empresa] = general_report.disponibilidad_promedio_ponderada_porcentage / 100
                row[lb_empresa] = general_report.nombre
                results.append(pool.apply_async(self.process_this_node_detail_report,
                                                kwds={"row": row.copy(), "detail_report": detail_report}))
                n_threads += 1

            report_log.info(f"({self.fecha_inicio}, {self.fecha_final}) Se han desplegado {n_threads} threads")
            pool.close()
            pool.join()
            for result in results:
                rows = result.get()
                df_details = df_details.append(rows, ignore_index=True)
                report_log.info(f"({self.fecha_inicio}, {self.fecha_final}) Nuevas filas añadidas")

            report_log.info(f"({self.fecha_inicio}, {self.fecha_final}) Reporte finalizado {self.id_report}")
            df_summary = df_summary.where(pd.notnull(df_summary), None)
            df_details = df_details.where(pd.notnull(df_details), None)
            df_novedades = df_novedades.where(pd.notnull(df_novedades), None)
            resp = True, df_summary, df_details, df_novedades, "Información correcta"
            if q is not None:
                q.put(resp)
            return resp

        except Exception as e:
            resp = False, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), f"Problemas al procesar la información \n {e}"
            if q is not None:
                q.put(resp)
            return resp
