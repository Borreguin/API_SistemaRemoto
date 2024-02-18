import uuid

from mongoengine import EmbeddedDocument, StringField, ListField, EmbeddedDocumentField, IntField, FloatField

from app.db.constants import consignacion_total_procentaje
from app.db.v2.entities.v2_sRConsignment import V2SRConsignment
from app.db.v2.entities.v2_sREntity import V2SREntity
from app.db.v2.v2SRNodeReport.Details.v2_sRInstallationReportDetails import V2SRInstallationReportDetails
from app.utils.utils import validate_percentage


class V2SREntityReportDetails(EmbeddedDocument):
    document_id = StringField(required=False)
    entidad_nombre = StringField(required=True)
    entidad_tipo = StringField(required=True)
    reportes_instalaciones = ListField(EmbeddedDocumentField(V2SRInstallationReportDetails), required=True, default=list())
    numero_tags = IntField(required=True, default=0)
    numero_tags_procesadas = IntField(required=True, default=0)
    tags_fallidas = ListField(StringField(), default=[])
    consignaciones = ListField(EmbeddedDocumentField(V2SRConsignment), required=False, default=list())
    consignaciones_acumuladas_minutos = IntField(required=True, default=0)
    periodo_evaluacion_minutos = IntField(required=True, default=0)
    periodo_efectivo_minutos = IntField(required=True, default=0)
    # el periodo real a evaluar = periodo_evaluacion_minutos - consignaciones_acumuladas_minutos
    # se permite el valor de -1 en caso que sea indefinida la disponibilidad:
    # esto ocurre por ejemplo en el caso que la totalidad del periodo evaluado está consignado
    disponibilidad_promedio_ponderada_minutos = FloatField(required=True, min_value=-1, default=0)
    disponibilidad_promedio_ponderada_porcentage = FloatField(required=True, min_value=-1, max_value=100, default=0)
    disponibilidad_promedio_porcentage = FloatField(required=True, min_value=-1, max_value=100, default=0)
    nota = StringField(required=False, default='Normal')
    ponderacion = FloatField(required=True, min_value=0, max_value=1, default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.document_id is None:
            self.document_id = f"{uuid.uuid4()}"

    def set_values(self, entity: V2SREntity):
        self.entidad_tipo = entity.entidad_tipo
        self.entidad_nombre = entity.entidad_nombre
        self.numero_tags = entity.n_tags

    def representation(self):
        return f'{self.entidad_nombre}'

    def consignacion_total(self):
        self.nota = f'Consignación total de {self.entidad_tipo} {self.entidad_nombre}'
        self.periodo_efectivo_minutos = 0
        self.disponibilidad_promedio_porcentage = consignacion_total_procentaje
        self.disponibilidad_promedio_ponderada_porcentage = consignacion_total_procentaje
        self.disponibilidad_promedio_ponderada_minutos = 0
        self.numero_tags_procesadas = 0
        self.ponderacion = 0
        return

    def calculate(self):
        self.numero_tags = sum([rb.numero_tags for rb in self.reportes_instalaciones if rb.numero_tags > 0])
        self.numero_tags_procesadas = sum([rb.numero_tags_procesadas for rb in self.reportes_instalaciones])
        self.consignaciones_acumuladas_minutos = sum([c.t_minutos for c in self.consignaciones if c.t_minutos > 0])
        self.periodo_efectivo_minutos = self.periodo_evaluacion_minutos - self.consignaciones_acumuladas_minutos

        if self.periodo_efectivo_minutos <= 0:
            self.consignacion_total()

        if self.consignaciones_acumuladas_minutos < self.periodo_evaluacion_minutos - self.periodo_efectivo_minutos:
            self.nota = 'Se considera consignaciones a nivel superior'
            self.consignaciones_acumuladas_minutos = self.periodo_evaluacion_minutos - self.periodo_efectivo_minutos

        n_valid_reports = sum([1 for rb in self.reportes_instalaciones if rb.periodo_efectivo_minutos > 0])
        if len(self.reportes_instalaciones) == 0 or self.periodo_efectivo_minutos == 0 or n_valid_reports == 0:
            self.disponibilidad_promedio_ponderada_porcentage = -1
            self.disponibilidad_promedio_ponderada_minutos = -1
            return
        avg_acc = 0
        for i_report in self.reportes_instalaciones:
            if i_report.periodo_efectivo_minutos > 0:
                i_report.ponderacion = i_report.numero_tags_procesadas/self.numero_tags_procesadas
                self.disponibilidad_promedio_ponderada_porcentage += i_report.ponderacion * i_report.disponibilidad_promedio_porcentage
                self.disponibilidad_promedio_ponderada_minutos += i_report.ponderacion * i_report.disponibilidad_promedio_minutos
                avg_acc += i_report.disponibilidad_promedio_porcentage
                self.tags_fallidas += i_report.tags_fallidas
            else:
                i_report.ponderacion = 0
        self.disponibilidad_promedio_ponderada_porcentage = validate_percentage(self.disponibilidad_promedio_ponderada_porcentage)
        self.disponibilidad_promedio_porcentage = validate_percentage(avg_acc / n_valid_reports)

    def __str__(self):
        return f"{self.entidad_tipo}:{self.entidad_nombre} [{len(self.reportes_instalaciones)}] utrs " \
               f"[{str(self.numero_tags)}] tags. " \
               f"(%disp_avg_pond:{round(self.disponibilidad_promedio_ponderada_porcentage, 3)} " \
               f" min_avg_pond:{round(self.disponibilidad_promedio_ponderada_minutos, 1)})"

    def to_dict(self):
        return dict(document_id=self.document_id, entidad_nombre=self.entidad_nombre, entidad_tipo=self.entidad_tipo, numero_tags=self.numero_tags,
                    reportes_instalaciones=[r.to_dict() for r in self.reportes_instalaciones],
                    disponibilidad_promedio_ponderada_porcentage=self.disponibilidad_promedio_ponderada_porcentage,
                    disponibilidad_promedio_ponderada_minutos=self.disponibilidad_promedio_ponderada_minutos,
                    periodo_evaluacion_minutos=self.periodo_evaluacion_minutos,
                    ponderacion=self.ponderacion)