import flask_app.settings.LogDeafultConfig
from flask_app.dto.mongo_engine_handler.SRNodeReport.SRNodeReportTemporal import SRNodeDetailsTemporal
from flask_app.dto.mongo_engine_handler.SRNodeReport.sRNodeReportBase import SRNodeDetailsBase
from flask_app.dto.mongo_engine_handler.sRNode import *
import hashlib
from flask_app.my_lib import utils as u
from flask_app.dto.mongo_engine_handler.SRNodeReport.sRNodeReportPermanente import SRNodeDetailsPermanente
from multiprocessing.pool import ThreadPool
import queue

lb_fecha_ini = "Fecha inicial"
lb_fecha_fin = "Fecha final"
lb_empresa = "Empresa"
lb_unidad_negocio = "Unidad de Negocio"
lb_utr = "UTR"
lb_utr_id = "UTR ID"
lb_protocolo = "Protocolo"
lb_dispo_ponderada_empresa = "Disponibilidad ponderada Empresa"
lb_dispo_ponderada_unidad = "Disponibilidad ponderada Unidad de Negocio"
lb_dispo_promedio_utr = "Disponibilidad promedio UTR"
lb_no_seniales = "No. señales"
lb_falladas = "Falladas"
lb_latitud = "Latitud"
lb_longitud = "Longitud"
lb_tag_name = "tag_name"
lb_indisponible_minutos = "indisponible_minutos"
lb_indisponible_minutos_promedio = "indisponible_minutos_promedio"
lb_periodo_evaluacion = "Periodo evaluación"
details_columns = [lb_fecha_ini, lb_fecha_fin, lb_empresa, lb_unidad_negocio, lb_utr, lb_protocolo,
                   lb_dispo_ponderada_empresa,
                   lb_dispo_ponderada_unidad, lb_dispo_promedio_utr, lb_no_seniales, lb_latitud,
                   lb_longitud]

log = flask_app.settings.LogDeafultConfig.LogDefaultConfig("sRFinalReportBase.log").logger


class SRNodeSummaryReport(EmbeddedDocument):
    id_report = StringField(required=True)
    nombre = StringField(required=True)
    tipo = StringField(required=True)
    # el valor -1 es aceptado en el caso de que la disponibilidad no este definida
    disponibilidad_promedio_ponderada_porcentage = FloatField(required=True, min_value=-1, max_value=100)
    procesamiento = DictField(required=True, default=dict())
    novedades = DictField(required=True, default=dict())
    tiempo_calculo_segundos = FloatField(required=False)
    actualizado = DateTimeField(default=dt.datetime.now())

    def to_dict(self):
        return dict(id_report=self.id_report, nombre=self.nombre, tipo=self.tipo,
                    disponibilidad_promedio_ponderada_porcentage=self.disponibilidad_promedio_ponderada_porcentage,
                    procesamiento=self.procesamiento, novedades=self.novedades,
                    tiempo_calculo_segundos=self.tiempo_calculo_segundos,
                    actualizado=str(self.actualizado))


class SRFinalReportBase(Document):
    id_report = StringField(required=True, unique=True)
    tipo = StringField(required=True, default="Reporte Sistema Remoto")
    fecha_inicio = DateTimeField(required=True)
    fecha_final = DateTimeField(required=True)
    periodo_evaluacion_minutos = IntField(required=True)
    # el valor -1 es aceptado en el caso de que la disponibilidad no este definida
    disponibilidad_promedio_ponderada_porcentage = FloatField(required=True, min_value=-1, max_value=100)
    disponibilidad_promedio_porcentage = FloatField(required=True, min_value=-1, max_value=100)
    reportes_nodos = ListField(EmbeddedDocumentField(SRNodeSummaryReport))
    reportes_nodos_detalle = ListField(ReferenceField(SRNodeDetailsBase, dbref=True), required=False)
    tiempo_calculo_segundos = FloatField(default=0)
    procesamiento = DictField(default=dict(numero_tags_total=0, numero_utrs_procesadas=0,
                                           numero_entidades_procesadas=0, numero_nodos_procesados=0))
    novedades = DictField(default=dict(tags_fallidas=0, utr_fallidas=0,
                                       entidades_fallidas=0, nodos_fallidos=0, detalle={}))
    actualizado = DateTimeField(default=dt.datetime.now())
    meta = {'allow_inheritance': True, 'abstract': True}
    # attributos utilizados para calculos internos:
    nodes_info = None
    utrs_dict = None

    def __init__(self, *args, **values):
        super().__init__(*args, **values)
        if self.id_report is None:
            id = str(self.tipo).lower().strip() + self.fecha_inicio.strftime('%d-%m-%Y %H:%M') + \
                 self.fecha_final.strftime('%d-%m-%Y %H:%M')
            self.id_report = hashlib.md5(id.encode()).hexdigest()
        t_delta = self.fecha_final - self.fecha_inicio
        self.periodo_evaluacion_minutos = t_delta.days * (60 * 24) + t_delta.seconds // 60 + (t_delta.seconds % 60)/60
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

    def append_node_summary_report(self, node_summary_report: SRNodeSummaryReport):
        self.reportes_nodos.append(node_summary_report)
        self.procesamiento["numero_tags_total"] += node_summary_report.procesamiento["numero_tags_total"]
        self.procesamiento["numero_utrs_procesadas"] += node_summary_report.procesamiento["numero_utrs_procesadas"]
        self.procesamiento["numero_entidades_procesadas"] += \
            node_summary_report.procesamiento["numero_entidades_procesadas"]
        self.novedades["tags_fallidas"] += len(node_summary_report.novedades["tags_fallidas"])
        self.novedades["utr_fallidas"] += len(node_summary_report.novedades["utr_fallidas"])
        self.novedades["entidades_fallidas"] += len(node_summary_report.novedades["entidades_fallidas"])
        if len(node_summary_report.novedades["tags_fallidas"]) > 0 \
                or len(node_summary_report.novedades["utr_fallidas"]) > 0 \
                or len(node_summary_report.novedades["utr_fallidas"]) > 0:
            nombre = str(node_summary_report.nombre).replace(".", "_").replace("$", "_")
            self.novedades["detalle"][nombre] = dict()
            # solo si existen novedades a reportar:
            if len(node_summary_report.novedades["tags_fallidas"]) > 0:
                self.novedades["detalle"][nombre]["tags_fallidas"] = \
                    len(node_summary_report.novedades["tags_fallidas"])
            if len(node_summary_report.novedades["utr_fallidas"]) > 0:
                self.novedades["detalle"][nombre]["utr_fallidas"] = \
                    len(node_summary_report.novedades["utr_fallidas"])
            if len(node_summary_report.novedades["entidades_fallidas"]) > 0:
                self.novedades["detalle"][nombre]["entidades_fallidas"] = \
                    len(node_summary_report.novedades["entidades_fallidas"])

    def calculate(self):
        if len(self.reportes_nodos) > 0:

            # cálculo de la disponibilidad promedio:
            self.disponibilidad_promedio_porcentage, n_reports, n_tags = 0, 0, 0
            for report in self.reportes_nodos:
                if report.disponibilidad_promedio_ponderada_porcentage > 0:
                    self.disponibilidad_promedio_porcentage += report.disponibilidad_promedio_ponderada_porcentage
                    n_reports += 1
                    n_tags += report.procesamiento['numero_tags_total']
            if n_reports > 0:
                self.disponibilidad_promedio_porcentage = self.disponibilidad_promedio_porcentage / n_reports
            else:
                # No se ha podido establecer la disponibilidad
                self.disponibilidad_promedio_porcentage = -1

            # cálculo de la disponibilidad promedio ponderada
            if n_tags > 0:
                self.disponibilidad_promedio_ponderada_porcentage = 0
                for rp in self.reportes_nodos:
                    if rp.disponibilidad_promedio_ponderada_porcentage > 0:
                        self.disponibilidad_promedio_ponderada_porcentage += \
                            rp.disponibilidad_promedio_ponderada_porcentage * rp.procesamiento[
                                "numero_tags_total"] / n_tags
                if self.disponibilidad_promedio_ponderada_porcentage > 100:
                    self.disponibilidad_promedio_ponderada_porcentage = 100

            # en el caso que no tenga tags internas válidas a calcular
            else:
                self.disponibilidad_promedio_ponderada_porcentage = -1
                self.disponibilidad_promedio_porcentage = -1

        # en el caso que no exista reportes de nodos:
        else:
            self.disponibilidad_promedio_ponderada_porcentage = -1
            self.disponibilidad_promedio_porcentage = -1
        reportes_nodos = sorted(self.reportes_nodos, key=lambda k: k["disponibilidad_promedio_ponderada_porcentage"])
        self.procesamiento["numero_nodos_procesados"] = len(self.reportes_nodos)
        self.reportes_nodos = reportes_nodos

    def to_dict(self):
        return dict(id_report=self.id_report, tipo=self.tipo, fecha_inicio=str(self.fecha_inicio),
                    fecha_final=str(self.fecha_final), periodo_evaluacion_minutos=self.periodo_evaluacion_minutos,
                    disponibilidad_promedio_ponderada_porcentage=self.disponibilidad_promedio_ponderada_porcentage,
                    disponibilidad_promedio_porcentage=self.disponibilidad_promedio_porcentage,
                    reportes_nodos=[r.to_dict() for r in self.reportes_nodos], procesamiento=self.procesamiento,
                    novedades=self.novedades, actualizado=str(self.actualizado),
                    tiempo_calculo_segundos=self.tiempo_calculo_segundos)

    def to_table(self):
        resp = dict(id_report=self.id_report, tipo=self.tipo, fecha_inicio=str(self.fecha_inicio),
                    fecha_final=str(self.fecha_final), periodo_evaluacion_minutos=self.periodo_evaluacion_minutos,
                    disponibilidad_promedio_ponderada_porcentage=self.disponibilidad_promedio_ponderada_porcentage,
                    disponibilidad_promedio_porcentage=self.disponibilidad_promedio_porcentage,
                    actualizado=str(self.actualizado),
                    tiempo_calculo_segundos=self.tiempo_calculo_segundos)
        resp.update(self.procesamiento)
        return resp

    def load_nodes_info(self):
        try:
            self.nodes_info = list()
            nodes_info = SRNode.objects().as_pymongo()
            self.nodes_info = [n for n in nodes_info]
        except Exception as e:
            log.error(f"{str(e)}")
            self.nodes_info = []
        return self

    def create_utrs_list(self):
        self.utrs_dict = dict()
        for node_info in self.nodes_info:
            for entidad in node_info["entidades"]:
                for utr in entidad["utrs"]:
                    self.utrs_dict[utr["id_utr"]] = utr
        return self.utrs_dict

    def process_this_node_detail_report(self, row:dict, detail_report):
        # log.info(f"Procesando reporte: {detail_report.nombre}")
        rows = list()
        for reporte_entidad in detail_report.reportes_entidades:
            # print(reporte_entidad.entidad_nombre)
            row[lb_dispo_ponderada_unidad] = reporte_entidad.disponibilidad_promedio_ponderada_porcentage / 100
            row[lb_unidad_negocio] = reporte_entidad.entidad_nombre
            for reporte_utr in reporte_entidad.reportes_utrs:
                row[lb_dispo_promedio_utr] = reporte_utr.disponibilidad_promedio_porcentage / 100
                row[lb_utr] = reporte_utr.utr_nombre
                row[lb_no_seniales] = reporte_utr.numero_tags
                row[lb_indisponible_minutos_promedio] = \
                    reporte_utr.indisponibilidad_acumulada_minutos / reporte_utr.numero_tags \
                        if reporte_utr.numero_tags > 0 else -1
                f_utr = self.utrs_dict.get(reporte_utr.id_utr, None)
                row[lb_protocolo] = f_utr.get("protocol", None) if f_utr is not None else None
                row[lb_latitud] = f_utr.get("latitude", None) if f_utr is not None else None
                row[lb_longitud] = f_utr.get("longitude", None) if f_utr is not None else None
                # log.info(f"Finalizando reporte: {self.id_report}, {reporte_entidad.entidad_nombre}")
                rows.append(row.copy())
        return rows

    def to_dataframe(self, utrs_dict: dict, q: queue.Queue = None):
        self.utrs_dict = utrs_dict
        try:
            # cola para recibir resultados
            log.info("Empezando la transformación a Dataframe")
            out_queue = queue.Queue()
            df_details = pd.DataFrame(columns=details_columns)
            summary = self.to_table()
            df_summary = pd.DataFrame(columns=list(summary.keys()))
            df_summary = df_summary.append(summary, ignore_index=True)
            df_novedades = pd.DataFrame(columns=["item", "tags_fallidas", "utr_fallidas", "entidades_fallidas",
                                                 "nodos_fallidos", "result", "log"], data=self.novedades_as_dict())
            df_novedades.set_index("item", inplace=True)
            row = {lb_fecha_ini: str(self.fecha_inicio), lb_fecha_fin: str(self.fecha_final)}
            # loading node info for each detail report
            n_threads = 0
            results = []
            n_pool = min(max(5, int(len(self.reportes_nodos)/5)), len(self.reportes_nodos))
            pool = ThreadPool(n_pool)
            log.info(f"Procesando la información de: {self.fecha_inicio}, {self.fecha_final}")
            for general_report in self.reportes_nodos:
                # recolectando reportes por cada nodo interno:
                if u.isTemporal(self.fecha_inicio, self.fecha_final):
                    detail_report = SRNodeDetailsTemporal.objects(id_report=general_report.id_report).first()
                else:
                    detail_report = SRNodeDetailsPermanente.objects(id_report=general_report.id_report).first()
                if detail_report is None:
                    log.warning(f"Este reporte no existe para este nodo: {general_report}")
                    continue
                # creating rows to process:
                row[lb_dispo_ponderada_empresa] = general_report.disponibilidad_promedio_ponderada_porcentage / 100
                row[lb_empresa] = general_report.nombre
                results.append(pool.apply_async(self.process_this_node_detail_report,
                                                kwds={"row": row.copy(), "detail_report": detail_report}))
                n_threads += 1

            log.info(f"({self.fecha_inicio}, {self.fecha_final}) Se han desplegado {n_threads} threads")
            pool.close()
            pool.join()
            for result in results:
                rows = result.get()
                df_details = df_details.append(rows, ignore_index=True)
                log.info(f"({self.fecha_inicio}, {self.fecha_final}) Nuevas filas añadidas")

            log.info(f"({self.fecha_inicio}, {self.fecha_final}) Reporte finalizado {self.id_report}")
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
