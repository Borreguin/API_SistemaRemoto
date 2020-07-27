from dto.mongo_engine_handler.sRNode import *
import hashlib


class SRNodeSummaryReport(EmbeddedDocument):
    id_report = StringField(required=True)
    nombre = StringField(required=True)
    tipo = StringField(required=True)
    disponibilidad_promedio_ponderada_porcentage = FloatField(required=True, min_value=0, max_value=100)
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


class SRFinalReport(Document):
    id_report = StringField(required=True, unique=True)
    tipo = StringField(required=True, default="Reporte Sistema Remoto")
    fecha_inicio = DateTimeField(required=True)
    fecha_final = DateTimeField(required=True)
    periodo_evaluacion_minutos = IntField(required=True)
    disponibilidad_promedio_ponderada_porcentage = FloatField(required=True, min_value=0, max_value=100)
    disponibilidad_promedio_porcentage = FloatField(required=True, min_value=0, max_value=100)
    reportes_nodos = ListField(EmbeddedDocumentField(SRNodeSummaryReport))
    tiempo_calculo_segundos = FloatField(default=0)
    procesamiento = DictField(default=dict(numero_tags_total=0, numero_utrs_procesadas=0,
                                           numero_entidades_procesadas=0, numero_nodos_procesados=0))
    novedades = DictField(default=dict(tags_fallidas=0, utr_fallidas=0,
                                       entidades_fallidas=0, nodos_fallidos=0, detalle={}))
    actualizado = DateTimeField(default=dt.datetime.now())
    meta = {"collection": "REPORT|FinalReports"}

    def __init__(self, *args, **values):
        super().__init__(*args, **values)
        id = str(self.tipo).lower().strip() + self.fecha_inicio.strftime('%d-%m-%Y %H:%M') + \
             self.fecha_final.strftime('%d-%m-%Y %H:%M')
        self.id_report = hashlib.md5(id.encode()).hexdigest()
        t_delta = self.fecha_final - self.fecha_inicio
        self.periodo_evaluacion_minutos = t_delta.days * (60 * 24) + t_delta.seconds // 60 + t_delta.seconds % 60
        if self.actualizado is None:
            self.actualizado = dt.datetime.now()

    def append_node_summary_report(self, node_summary_report: SRNodeSummaryReport):
        self.reportes_nodos.append(node_summary_report)
        self.procesamiento["numero_tags_total"] += node_summary_report.procesamiento["numero_tags_total"]
        self.procesamiento["numero_utrs_procesadas"] += node_summary_report.procesamiento["numero_utrs_procesadas"]
        self.procesamiento["numero_entidades_procesadas"] += node_summary_report.procesamiento[
            "numero_entidades_procesadas"]
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
            self.disponibilidad_promedio_porcentage = \
                sum([rp.disponibilidad_promedio_ponderada_porcentage for rp in self.reportes_nodos]) / len(
                    self.reportes_nodos)
            n = self.procesamiento["numero_tags_total"]
            if n > 0:
                self.disponibilidad_promedio_ponderada_porcentage = \
                    sum([rp.disponibilidad_promedio_ponderada_porcentage * rp.procesamiento["numero_tags_total"] / n
                         for rp in self.reportes_nodos])

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

