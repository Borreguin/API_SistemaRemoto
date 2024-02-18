from __future__ import annotations
from mongoengine import EmbeddedDocument, StringField, FloatField, DictField, DateTimeField
import datetime as dt

from app.db.v2.v2SRFinalReport.constants import *
from app.db.v2.v2SRNodeReport.V2SRNodeDetailsPermanent import V2SRNodeDetailsPermanent
from app.db.v2.v2SRNodeReport.V2SRNodeDetailsTemporal import V2SRNodeDetailsTemporal


class V2SRNodeSummaryReport(EmbeddedDocument):
    id_report = StringField(required=True)
    id_node = StringField(required=False)
    nombre = StringField(required=True)
    tipo = StringField(required=True)
    # el valor -1 es aceptado en el caso de que la disponibilidad no este definida
    disponibilidad_promedio_ponderada_porcentage = FloatField(required=True, min_value=-1, max_value=100)
    procesamiento = DictField(required=True, default=dict())
    novedades = DictField(required=True, default=dict())
    tiempo_calculo_segundos = FloatField(required=False)
    actualizado = DateTimeField(default=dt.datetime.now())

    def to_dict(self):
        return dict(id_report=self.id_report,id_node=self.id_node, nombre=self.nombre, tipo=self.tipo,
                    disponibilidad_promedio_ponderada_porcentage=self.disponibilidad_promedio_ponderada_porcentage,
                    procesamiento=self.procesamiento, novedades=self.novedades,
                    tiempo_calculo_segundos=self.tiempo_calculo_segundos,
                    actualizado=str(self.actualizado))

    def set_values_from_detail_report(self, d_report: V2SRNodeDetailsPermanent| V2SRNodeDetailsTemporal):
        self.id_report = d_report.id_report
        self.id_node = d_report.id_node
        self.nombre = d_report.nombre
        self.tipo = d_report.tipo
        self.disponibilidad_promedio_ponderada_porcentage = d_report.disponibilidad_promedio_ponderada_porcentage
        self.set_procesamiento(d_report)
        self.set_novedades(d_report)
        self.tiempo_calculo_segundos = max(d_report.tiempo_calculo_segundos, 1)
        return self

    def set_procesamiento(self, d_report:  V2SRNodeDetailsPermanent| V2SRNodeDetailsTemporal):
        to_work_with = [lb_numero_tags, lb_numero_tags_procesadas, lb_numero_bahias_procesadas,
                        lb_numero_instalaciones_procesadas, lb_numero_entidades_procesadas]
        result = dict()
        for att in to_work_with:
            result[att] = getattr(d_report, att, 0)
        self.procesamiento = result

    def set_novedades(self, d_report:  V2SRNodeDetailsPermanent| V2SRNodeDetailsTemporal):
        to_work_with = [lb_tags_fallidas, lb_bahias_fallidas, lb_instalaciones_fallidas, lb_entidades_fallidas]
        result = dict()
        for att in to_work_with:
            result[att] = len(getattr(d_report, att, []))
        self.novedades = result