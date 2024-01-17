from mongoengine import Document, StringField, LazyReferenceField, IntField, DateTimeField, ListField, \
    EmbeddedDocumentField, FloatField, DictField

from app.common.util import get_time_in_minutes, clean_alphanumeric_name
from app.db.v2.entities.v2_sRConsignment import V2SRConsignment
from app.db.v2.entities.v2_sRNode import V2SRNode
from app.db.v2.v2SRNodeReport.Details.v2_sREntityReportDetails import V2SREntityReportDetails
from app.db.v2.v2SRNodeReport.report_util import get_report_id
import datetime as dt

from app.utils.utils import validate_percentage


class V2SRNodeDetailsBase(Document):
    id_report = StringField(required=True, unique=True)
    nodo = LazyReferenceField(V2SRNode, required=True, dbref=True, passthrough=False)
    nombre = StringField(required=True, default=None)
    tipo = StringField(required=True, default=None)
    periodo_evaluacion_minutos = IntField(required=True)
    periodo_efectivo_minutos = IntField(required=True, default=0)
    fecha_inicio = DateTimeField(required=True)
    fecha_final = DateTimeField(required=True)
    numero_tags = IntField(required=True, default=0)
    numero_tags_procesadas = IntField(required=True, default=0)
    reportes_entidades = ListField(EmbeddedDocumentField(V2SREntityReportDetails), required=True, default=list())
    consignaciones = ListField(EmbeddedDocumentField(V2SRConsignment), required=False, default=list())
    consignaciones_acumuladas_minutos = IntField(required=True, default=0)
    consignaciones_internas = ListField(EmbeddedDocumentField(V2SREntityReportDetails), required=False, default=list())
    # se acepta el caso de -1 para indicar que la disponibilidad no pudo ser establecida
    disponibilidad_promedio_ponderada_porcentage = FloatField(required=True, min_value=-1, max_value=100)
    disponibilidad_promedio_porcentage = FloatField(required=True, min_value=-1, max_value=100)
    tiempo_calculo_segundos = FloatField(required=False)
    tags_fallidas_detalle = DictField(default={}, required=False)
    tags_fallidas = ListField(StringField(), default=[])
    bahias_fallidas = ListField(DictField(), default=[], required=False)
    instalaciones_fallidas = ListField(DictField(), default=[], required=False)
    entidades_fallidas = ListField(DictField(), default=[], required=False)
    numero_bahias_procesadas = IntField(required=False, default=0)
    numero_instalaciones_procesadas = IntField(required=False, default=0)
    numero_entidades_procesadas = IntField(required=False, default=0)
    actualizado = DateTimeField(default=dt.datetime.now())
    ponderacion = FloatField(required=True, min_value=0, max_value=1, default=1)
    nota = StringField(required=False, default='Normal')
    meta = {'allow_inheritance': True,'abstract':True}

    def __init__(self, *args, **values):
        super().__init__(*args, **values)
        if self.nombre is not None and self.tipo is not None:
            self.id_report = get_report_id(self.tipo, self.nombre, self.fecha_inicio, self.fecha_final)

    def __str__(self):
        return f"({self.tipo}, {self.nombre}):[ent:{len(self.reportes_entidades)}, tags:{self.numero_tags}]"

    def representation(self):
        return f'{self.nombre}'

    def calculate(self):
        # en caso que la disponibilidad de una entidad sea -1, significa que ha sido consignado en su totalidad
        # o que no se puede calcular ya que no tiene tags correctas para el cálculo:
        # para la ponderación solo se usarán aquellas que estan disponibles:
        self.periodo_evaluacion_minutos = get_time_in_minutes(self.fecha_inicio, self.fecha_final)
        self.numero_tags = sum([rb.numero_tags for rb in self.reportes_entidades])
        self.numero_tags_procesadas = sum([rb.numero_tags_procesadas for rb in self.reportes_entidades])
        self.consignaciones_acumuladas_minutos = sum([c.t_minutos for c in self.consignaciones if c.t_minutos > 0])
        self.periodo_efectivo_minutos = self.periodo_evaluacion_minutos - self.consignaciones_acumuladas_minutos
        if len(self.consignaciones_internas) > 0:
            self.nota = f'Contiene consignaciones internas'

        n_valid_report = sum([1 for rb in self.reportes_entidades if rb.disponibilidad_promedio_porcentage > 0])
        if (len(self.reportes_entidades) == 0 or self.periodo_efectivo_minutos == 0 or
                self.numero_tags_procesadas == 0 or n_valid_report == 0):
            self.disponibilidad_promedio_ponderada_porcentage = -1
            self.disponibilidad_promedio_porcentage = -1
            self.nota = f'No es posible calcular este nodo'
            return

        avg_acc, weight_avg = 0, 0
        for e_report in self.reportes_entidades:
            if e_report.disponibilidad_promedio_porcentage > 0:
                avg_acc += e_report.disponibilidad_promedio_porcentage
                e_report.ponderacion = e_report.numero_tags_procesadas / self.numero_tags_procesadas
                weight_avg +=  e_report.ponderacion * e_report.disponibilidad_promedio_porcentage
                self.tags_fallidas += e_report.tags_fallidas

        self.disponibilidad_promedio_porcentage = validate_percentage(avg_acc/ n_valid_report)
        self.disponibilidad_promedio_ponderada_porcentage = validate_percentage(weight_avg)
        self.get_failed_tag_details()


    def get_failed_tag_details(self):
        result = dict()
        if self.reportes_entidades is not None and len(self.reportes_entidades) > 0:
            for e_report in self.reportes_entidades:
                i_reports, i_details = build_dict(e_report, 'reportes_instalaciones', result)
                for i_report in i_reports:
                    key = clean_alphanumeric_name(i_report.representation())
                    b_reports, details = build_dict(i_report, 'reportes_bahias', i_details[key], True)
                    result[key] = details
        self.tags_fallidas_detalle = result

    def to_dict(self):
        return dict(id_node=str(self.nodo.id), id_report=self.id_report, tipo=self.tipo, nombre=self.nombre,
                    fecha_inicio=str(self.fecha_inicio),
                    fecha_final=str(self.fecha_final), actualizado=str(self.actualizado),
                    tiempo_calculo_segundos=self.tiempo_calculo_segundos,
                    periodo_evaluacion_minutos=self.periodo_evaluacion_minutos,
                    periodo_efectivo_minutos=self.periodo_efectivo_minutos,
                    numero_tags=self.numero_tags, numero_tags_procesadas=self.numero_tags_procesadas,
                    tags_fallidas=self.tags_fallidas, tags_fallidas_detalle=self.tags_fallidas_detalle,
                    consignaciones=[c.to_dict() for c in self.consignaciones] if self.consignaciones is not None else [],
                    consignaciones_internas=[c.to_dict() for c in self.consignaciones_internas] if self.consignaciones_internas is not None else [],
                    consignaciones_acumuladas_minutos= self.consignaciones_acumuladas_minutos,
                    disponibilidad_promedio_ponderada_porcentage=self.disponibilidad_promedio_ponderada_porcentage,
                    disponibilidad_promedio_porcentage=self.disponibilidad_promedio_porcentage,
                    entidades_fallidas=self.entidades_fallidas,
                    ponderacion=self.ponderacion,
                    reportes_entidades=[r.to_dict() for r in self.reportes_entidades])

    def to_summary(self):
        novedades=dict(tags_fallidas=self.tags_fallidas, instalaciones_fallidas=self.instalaciones_fallidas, entidades_fallidas=self.entidades_fallidas)
        n_entidades = len(self.reportes_entidades)
        n_instalaciones = sum([len(e.reportes_instalaciones) for e in self.reportes_entidades if e.disponibilidad_promedio_porcentage > 0])
        procesamiento=dict(numero_tags=self.numero_tags, numero_instalaciones_procesadas=n_instalaciones,
                           numero_entidades_procesadas=n_entidades)
        return dict(id_report=self.id_report, nombre=self.nombre, tipo=self.tipo,
                    disponibilidad_promedio_ponderada_porcentage=self.disponibilidad_promedio_ponderada_porcentage,
                    disponibilidad_promedio_porcentage=self.disponibilidad_promedio_porcentage,
                    novedades=novedades, procesamiento=procesamiento, actualizado=self.actualizado,
                    tiempo_calculo_segundos=self.tiempo_calculo_segundos)

def build_dict(report, next_level:str, result:dict, is_bahia:bool = False):
    values = getattr(report, next_level)
    reports = list()
    if values is not None and len(values) > 0:
        for value in values:
            key = clean_alphanumeric_name(value.representation())
            if is_bahia and len(value.tags_fallidas) > 0:
                result[key] = value.tags_fallidas
            if value.tags_fallidas is not None and len(value.tags_fallidas) > 0:
                if result.get(key, None) is None:
                    result[key] = dict()
                reports.append(value)
        return reports, result
    return None, result




