from __future__ import annotations
from mongoengine import EmbeddedDocument, StringField, FloatField, DictField, DateTimeField
import datetime as dt

from app.db.v2.v2SRFinalReport.constants import *
from app.db.v2.v2SRNodeReport.V2SRNodeDetailsPermanent import V2SRNodeDetailsPermanent
from app.db.v2.v2SRNodeReport.V2SRNodeDetailsTemporal import V2SRNodeDetailsTemporal


class V2SRNodeSummaryReport(EmbeddedDocument):
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

    def set_values_from_detail_report(self, d_report: V2SRNodeDetailsPermanent| V2SRNodeDetailsTemporal):
        self.id_report = d_report.id_report
        self.nombre = d_report.nombre
        self.tipo = d_report.tipo
        self.disponibilidad_promedio_ponderada_porcentage = d_report.disponibilidad_promedio_ponderada_porcentage
        self.procesamiento = self.get_procesamiento()
        self.novedades = self.get_novedades()
        self.tiempo_calculo_segundos = d_report.tiempo_calculo_segundos
        return self

    def get_procesamiento(self):
        to_work_with = [lb_numero_tags, lb_numero_tags_procesadas, lb_numero_bahias_procesadas,
                        lb_numero_instalaciones_procesadas, lb_numero_entidades_procesadas]
        result = dict()
        for att in to_work_with:
            result[att] = getattr(self, att, 0)
        return result

    def get_novedades(self, d_report:  V2SRNodeDetailsPermanent| V2SRNodeDetailsTemporal):
        to_work_with = [lb_tags_fallidas, lb_bahias_fallidas, lb_instalaciones_fallidas, lb_entidades_fallidas]
        result = dict()
        for att in to_work_with:
            result[att] = len(getattr(d_report, att, []))
        return result