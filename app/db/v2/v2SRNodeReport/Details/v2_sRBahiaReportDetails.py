from typing import List

from mongoengine import EmbeddedDocument, StringField, ListField, EmbeddedDocumentField, IntField, FloatField

from app.db.v2.entities.v2_sRBahia import V2SRBahia
from app.db.v2.entities.v2_sRConsignment import V2SRConsignment
from app.db.v2.v2SRNodeReport.Details.v2_sRTagReportDetails import V2SRTagDetails
from app.utils.utils import validate_percentage


class V2SRBahiaReportDetails(EmbeddedDocument):
    document_id = StringField(required=True)
    bahia_code = StringField(required=True)
    voltaje = StringField(required=True)
    bahia_nombre = StringField(required=True)
    reportes_tags = ListField(EmbeddedDocumentField(V2SRTagDetails), required=False, default=list())
    numero_tags = IntField(required=True, default=0)
    numero_tags_procesadas = IntField(required=True, default=0)
    tags_fallidas = ListField(StringField(), default=[])
    periodo_evaluacion_minutos = IntField(required=True, default=0)
    periodo_efectivo_minutos = IntField(required=True, default=0)
    # el periodo real a evaluar = periodo_evaluacion_minutos - consignaciones_acumuladas_minutos
    # se permite el valor de -1 en caso que sea indefinida la disponibilidad:
    # esto ocurre por ejemplo en el caso que la totalidad del periodo evaluado está consignado
    indisponibilidad_acumulada_minutos = IntField(required=True, min_value=-1, default=-1)
    indisponibilidad_promedio_minutos = IntField(required=True, min_value=-1, default=-1)
    disponibilidad_promedio_minutos = IntField(required=True, min_value=-1, default=-1)
    disponibilidad_promedio_porcentage = FloatField(required=True, min_value=-1, max_value=100, default=-1)
    consignaciones = ListField(EmbeddedDocumentField(V2SRConsignment), required=False, default=[])
    consignaciones_acumuladas_minutos = IntField(required=True, default=0)
    nota = StringField(required=False, default='Normal')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    def set_values(self, bahia: V2SRBahia, periodo_minutos: int, consignaciones: List[V2SRConsignment]):
        self.document_id = bahia.get_document_id()
        self.bahia_code = bahia.bahia_code
        self.voltaje = str(bahia.voltaje)
        self.bahia_nombre = bahia.bahia_nombre
        self.periodo_evaluacion_minutos = periodo_minutos
        self.consignaciones = consignaciones

    def __str__(self):
        return (f"{self.bahia_nombre} ({self.voltaje}): {self.numero_tags} nTags  "
                f"(eval:{self.periodo_evaluacion_minutos} - cnsg:{self.consignaciones_acumuladas_minutos} = "
                f"eftv:{self.periodo_efectivo_minutos} => disp_avg:{round(self.disponibilidad_promedio_minutos, 1)} "
                f"%disp: {round(self.disponibilidad_promedio_porcentage, 2)})")

    def representation(self):
        return f'{self.bahia_code if len(self.bahia_nombre) > 0 else self.bahia_nombre}'

    def calculate(self):
        if self.reportes_tags is not None and len(self.reportes_tags) > 0:
            self.numero_tags_procesadas = len(self.reportes_tags)
            self.indisponibilidad_acumulada_minutos = sum([tr.indisponible_minutos for tr in self.reportes_tags])
            self.indisponibilidad_promedio_minutos = round(self.indisponibilidad_acumulada_minutos / self.numero_tags_procesadas, 3)
            if self.periodo_efectivo_minutos > 0:
                self.disponibilidad_promedio_minutos = self.periodo_efectivo_minutos - self.indisponibilidad_promedio_minutos
                self.disponibilidad_promedio_porcentage = (self.disponibilidad_promedio_minutos / self.periodo_efectivo_minutos) * 100
                self.disponibilidad_promedio_porcentage = validate_percentage(self.disponibilidad_promedio_porcentage)
        if len(self.consignaciones) > 0:
            self.consignaciones_acumuladas_minutos = sum([c.t_minutos for c in self.consignaciones])
        if self.consignaciones_acumuladas_minutos < self.periodo_evaluacion_minutos - self.periodo_efectivo_minutos:
            self.nota = 'Se considera consignaciones a nivel superior'
            self.consignaciones_acumuladas_minutos = self.periodo_evaluacion_minutos - self.periodo_efectivo_minutos
        self.numero_tags = len(self.reportes_tags) + len(self.tags_fallidas)