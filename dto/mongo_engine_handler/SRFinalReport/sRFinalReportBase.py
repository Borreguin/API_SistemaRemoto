from dto.mongo_engine_handler.SRNodeReport.SRNodeReportTemporal import SRNodeDetailsTemporal
from dto.mongo_engine_handler.SRNodeReport.sRNodeReportBase import SRNodeDetailsBase
from dto.mongo_engine_handler.sRNode import *
import hashlib
from my_lib import utils as u
from dto.mongo_engine_handler.SRNodeReport.sRNodeReportPermanente import SRNodeDetailsPermanente

lb_fecha_ini = "Fecha inicial"
lb_fecha_fin = "Fecha final"
lb_empresa = "Empresa"
lb_unidad_negocio = "Unidad de Negocio"
lb_utr = "UTR"
lb_utr_id = "UTR ID"
lb_protocolo = "Protocolo"
lb_disponibilidad_ponderada_empresa = "Disponibilidad ponderada Empresa"
lb_disponibilidad_ponderada_unidad = "Disponibilidad ponderada Unidad de Negocio"
lb_disponibilidad_promedio_utr = "Disponibilidad promedio UTR"
lb_no_seniales = "No. señales"
lb_falladas = "Falladas"
lb_latitud = "Latitud"
lb_longitud = "Longitud"
lb_tag_name = "tag_name"
lb_indisponible_minutos = "indisponible_minutos"
lb_indisponible_minutos_promedio = "indisponible_minutos_promedio"
lb_periodo_evaluacion = "Periodo evaluación"
details_columns = [lb_fecha_ini, lb_fecha_fin, lb_empresa, lb_unidad_negocio, lb_utr, lb_protocolo, lb_disponibilidad_ponderada_empresa,
           lb_disponibilidad_ponderada_unidad, lb_disponibilidad_promedio_utr, lb_no_seniales, lb_latitud, lb_longitud]


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
    #TODO: Revisar posible problema de referencia SRNodeDetalilsBase
    reportes_nodos_detalle = ListField(ReferenceField(SRNodeDetailsBase, dbref=True), required=False)
    tiempo_calculo_segundos = FloatField(default=0)
    procesamiento = DictField(default=dict(numero_tags_total=0, numero_utrs_procesadas=0,
                                           numero_entidades_procesadas=0, numero_nodos_procesados=0))
    novedades = DictField(default=dict(tags_fallidas=0, utr_fallidas=0,
                                       entidades_fallidas=0, nodos_fallidos=0, detalle={}))
    actualizado = DateTimeField(default=dt.datetime.now())
    meta = {'allow_inheritance': True,'abstract':True}

    def __init__(self, *args, **values):
        super().__init__(*args, **values)
        if self.id_report is None:
            id = str(self.tipo).lower().strip() + self.fecha_inicio.strftime('%d-%m-%Y %H:%M') + \
                 self.fecha_final.strftime('%d-%m-%Y %H:%M')
            self.id_report = hashlib.md5(id.encode()).hexdigest()
        t_delta = self.fecha_final - self.fecha_inicio
        self.periodo_evaluacion_minutos = t_delta.days * (60 * 24) + t_delta.seconds // 60 + t_delta.seconds % 60
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
                self.disponibilidad_promedio_porcentage = self.disponibilidad_promedio_porcentage/n_reports
            else:
                # No se ha podido establecer la disponibilidad
                self.disponibilidad_promedio_porcentage = -1

            # cálculo de la disponibilidad promedio ponderada
            if n_tags > 0:
                self.disponibilidad_promedio_ponderada_porcentage = 0
                for rp in self.reportes_nodos:
                    if rp.disponibilidad_promedio_ponderada_porcentage > 0:
                        self.disponibilidad_promedio_ponderada_porcentage += \
                            rp.disponibilidad_promedio_ponderada_porcentage * rp.procesamiento["numero_tags_total"] / n_tags
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

    def to_dataframe(self):
        try:
            df_details = pd.DataFrame(columns=details_columns)
            summary = self.to_table()
            df_summary = pd.DataFrame(columns=list(summary.keys()))
            df_summary = df_summary.append(summary, ignore_index=True)
            df_novedades = pd.DataFrame(columns=["item", "tags_fallidas", "utr_fallidas", "entidades_fallidas",
                                                 "nodos_fallidos", "result", "log"], data=self.novedades_as_dict())
            df_novedades.set_index("item", inplace=True)
            row = {lb_fecha_ini:str(self.fecha_inicio), lb_fecha_fin: str(self.fecha_final)}
            for reporte in self.reportes_nodos:
                row[lb_disponibilidad_ponderada_empresa] = reporte.disponibilidad_promedio_ponderada_porcentage/100
                row[lb_empresa] = reporte.nombre
                if u.isTemporal(self.fecha_inicio,self.fecha_final):
                    node_report_db = SRNodeDetailsTemporal.objects(id_report=reporte.id_report).first()
                else:
                    node_report_db = SRNodeDetailsPermanente.objects(id_report=reporte.id_report).first()
                nodo = node_report_db.nodo.fetch()
                entidades = nodo.entidades
                utrs = list()
                for entidad in entidades:
                    utrs += entidad.utrs
                if node_report_db is None:
                    # No se encontró reporte asociado al Nodo
                    continue
                for reporte_entidad in node_report_db.reportes_entidades:
                    # print(reporte_entidad.entidad_nombre)
                    row[lb_disponibilidad_ponderada_unidad] = reporte_entidad.disponibilidad_promedio_ponderada_porcentage/100
                    row[lb_unidad_negocio] = reporte_entidad.entidad_nombre
                    for reporte_utr in reporte_entidad.reportes_utrs:
                        row[lb_disponibilidad_promedio_utr] = reporte_utr.disponibilidad_promedio_porcentage/100
                        row[lb_utr] = reporte_utr.utr_nombre
                        row[lb_no_seniales] = reporte_utr.numero_tags
                        row[lb_indisponible_minutos_promedio] = \
                            reporte_utr.indisponibilidad_acumulada_minutos / reporte_utr.numero_tags \
                                if reporte_utr.numero_tags > 0 else -1
                        f_utr = [utr for utr in utrs if reporte_utr.id_utr == utr.id_utr]
                        if len(f_utr) == 1:
                            row[lb_protocolo] = f_utr[0].protocol
                            row[lb_latitud] = f_utr[0].latitude
                            row[lb_longitud] = f_utr[0].longitude

                        df_details = df_details.append(row.copy(), ignore_index=True)
            df_summary = df_summary.where(pd.notnull(df_summary), None)
            df_details = df_details.where(pd.notnull(df_details), None)
            df_novedades = df_novedades.where(pd.notnull(df_novedades), None)
            return True, df_summary, df_details, df_novedades, "Información correcta"

        except Exception as e:
            return False, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), f"Problemas al procesar la información \n {e}"