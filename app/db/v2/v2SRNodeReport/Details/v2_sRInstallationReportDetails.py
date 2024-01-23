from typing import List

from mongoengine import EmbeddedDocument, StringField, IntField, ListField, EmbeddedDocumentField, FloatField

from app.db.v2.entities.v2_sRConsignment import V2SRConsignment
from app.db.v2.entities.v2_sRInstallation import V2SRInstallation
from app.db.v2.v2SRNodeReport.Details.v2_sRBahiaReportDetails import V2SRBahiaReportDetails
from app.utils.utils import validate_percentage


class V2SRInstallationReportDetails(EmbeddedDocument):
    document_id = StringField(required=True)
    instalacion_id = StringField(required=True)
    instalacion_ems_code=StringField(required=True)
    instalacion_nombre = StringField(required=True)
    instalacion_tipo = StringField(required=True)
    indisponibilidad_acumulada_minutos = IntField(required=True, default=0)
    indisponibilidad_promedio_minutos = IntField(required=True, default=0)
    reportes_bahias = ListField(EmbeddedDocumentField(V2SRBahiaReportDetails), required=False, default=list())
    consignaciones = ListField(EmbeddedDocumentField(V2SRConsignment),required=False, default=list())
    consignaciones_acumuladas_minutos = IntField(required=True, default=0)
    numero_tags = IntField(required=True, default=0)
    numero_tags_procesadas = IntField(required=True, default=0)
    tags_fallidas = ListField(StringField(), default=[])
    periodo_evaluacion_minutos = IntField(required=True)
    periodo_efectivo_minutos = IntField(required=True, default=0)
    # periodo_efectivo_minutos:
    # el periodo real a evaluar = periodo_evaluacion_minutos - consignaciones_acumuladas_minutos
    # se admite el valor de -1 para los casos en los que la disponibilidad queda indefinida
    # Ej: Cuando el periodo evaluado está consignado en su totalidad
    disponibilidad_promedio_minutos = IntField(required=True, min_value=-1, default=0)
    disponibilidad_promedio_porcentage = FloatField(required=True, min_value=-1, max_value=100, default=0)
    nota = StringField(required=True, default='Normal')
    ponderacion = FloatField(required=True, min_value=0, max_value=1, default=1)


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def set_values(self, installation: V2SRInstallation, consignaciones: List[V2SRConsignment]):
        self.instalacion_id = installation.instalacion_id
        self.instalacion_ems_code = installation.instalacion_ems_code
        self.instalacion_nombre = installation.instalacion_nombre
        self.instalacion_tipo = installation.instalacion_tipo
        self.document_id = installation.get_document_id()
        self.consignaciones = consignaciones


    def __str__(self):
        return f"{self.instalacion_nombre}: [{len(self.reportes_bahias)}] bahias " \
               f"(eval:{self.periodo_evaluacion_minutos} - cnsg:{self.consignaciones_acumuladas_minutos} = " \
               f" eftv:{self.periodo_efectivo_minutos} => disp_avg:{round(self.disponibilidad_promedio_minutos, 1)} " \
               f" %disp: {round(self.disponibilidad_promedio_porcentage, 2)})"

    def representation(self):
        return f'{self.instalacion_nombre}'

    def calculate(self):
        self.numero_tags = sum([rb.numero_tags for rb in self.reportes_bahias])
        self.numero_tags_procesadas = sum([rb.numero_tags_procesadas for rb in self.reportes_bahias])
        self.consignaciones_acumuladas_minutos = sum([c.t_minutos for c in self.consignaciones])

        self.periodo_efectivo_minutos = self.periodo_evaluacion_minutos - self.consignaciones_acumuladas_minutos
        self.indisponibilidad_promedio_minutos = int(self.indisponibilidad_acumulada_minutos / self.numero_tags_procesadas)

        if len(self.reportes_bahias) > 0 and self.numero_tags_procesadas > 0:
            self.indisponibilidad_acumulada_minutos = sum(
                [rb.indisponibilidad_acumulada_minutos for rb in self.reportes_bahias if rb.indisponibilidad_acumulada_minutos != -1]
            )
            self.indisponibilidad_promedio_minutos = self.indisponibilidad_acumulada_minutos / self.numero_tags_procesadas
            self.disponibilidad_promedio_minutos = sum(
                [rb.disponibilidad_promedio_minutos for rb in self.reportes_bahias if rb.disponibilidad_promedio_minutos != -1]
            )/len(self.reportes_bahias)

            self.disponibilidad_promedio_porcentage = sum(
                [rb.disponibilidad_promedio_porcentage for rb in self.reportes_bahias if rb.disponibilidad_promedio_porcentage != -1]
            )/len(self.reportes_bahias)
            self.disponibilidad_promedio_porcentage = validate_percentage(self.disponibilidad_promedio_porcentage)

        if self.consignaciones_acumuladas_minutos < self.periodo_evaluacion_minutos - self.periodo_efectivo_minutos:
            self.nota = 'Se considera consignaciones a nivel superior'
            self.consignaciones_acumuladas_minutos = self.periodo_evaluacion_minutos - self.periodo_efectivo_minutos

    def to_dict(self):
        return dict(id_utr=self.instalacion_id, nombre=self.instalacion_nombre, tipo=self.instalacion_tipo,
                    bahia_details=[t.to_dict() for t in self.reportes_bahias],
                    numero_bahias=len(self.reportes_bahias),
                    numero_tags=self.numero_tags,
                    numero_tags_procesadas=self.numero_tags_procesadas,
                    indisponibilidad_acumulada_minutos=self.indisponibilidad_acumulada_minutos,
                    consignaciones_acumuladas_minutos=self.consignaciones_acumuladas_minutos,
                    disponibilidad_promedio_porcentage=self.disponibilidad_promedio_porcentage,
                    ponderacion=self.ponderacion,
                    consignaciones=[c.to_dict() for c in self.consignaciones]
                    )
