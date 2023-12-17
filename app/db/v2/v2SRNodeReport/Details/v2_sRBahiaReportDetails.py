from mongoengine import EmbeddedDocument, StringField, ListField, EmbeddedDocumentField, IntField, FloatField

from app.db.v2.v2SRNodeReport.Details.v2_sRTagReportDetails import V2SRTagDetails


class V2SRBahiaReportDetails(EmbeddedDocument):
    document_id = StringField(required=True)
    bahia_code = StringField(required=True)
    voltaje = StringField(required=True)
    bahia_nombre = StringField(required=True)
    reportes_tags = ListField(EmbeddedDocumentField(V2SRTagDetails), required=True, default=list())
    numero_tags = IntField(required=True, default=0)
    periodo_evaluacion_minutos = IntField(required=True)
    # el periodo real a evaluar = periodo_evaluacion_minutos - consignaciones_acumuladas_minutos
    # se permite el valor de -1 en caso que sea indefinida la disponibilidad:
    # esto ocurre por ejemplo en el caso que la totalidad del periodo evaluado está consignado
    disponibilidad_promedio_minutos = FloatField(required=True, min_value=-1, max_value=100, default=-1)
