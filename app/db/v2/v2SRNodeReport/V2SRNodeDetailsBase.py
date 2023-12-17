from mongoengine import Document, StringField, LazyReferenceField, IntField, DateTimeField, ListField, \
    EmbeddedDocumentField, FloatField, DictField

from app.db.v2.entities.v2_sRConsignment import V2SRConsignment
from app.db.v2.entities.v2_sRNode import V2SRNode
from app.db.v2.v2SRNodeReport.Details.v2_sREntityReportDetails import V2SREntityReportDetails
from app.db.v2.v2SRNodeReport.report_util import get_report_id
import datetime as dt

class V2SRNodeDetailsBase(Document):
    id_report = StringField(required=True, unique=True)
    nodo = LazyReferenceField(V2SRNode, required=True, dbref=True, passthrough=False)
    nombre = StringField(required=True, default=None)
    tipo = StringField(required=True, default=None)
    periodo_evaluacion_minutos = IntField(required=True)
    fecha_inicio = DateTimeField(required=True)
    fecha_final = DateTimeField(required=True)
    numero_tags_total = IntField(required=True, default=0)
    reportes_entidades = ListField(EmbeddedDocumentField(V2SREntityReportDetails), required=True, default=list())
    consignaciones = ListField(EmbeddedDocumentField(V2SRConsignment), required=True, default=list())
    # se acepta el caso de -1 para indicar que la disponibilidad no pudo ser establecida
    disponibilidad_promedio_ponderada_porcentage = FloatField(required=True, min_value=-1, max_value=100)
    tiempo_calculo_segundos = FloatField(required=False)
    tags_fallidas_detalle = DictField(default={}, required=False)
    tags_fallidas = ListField(StringField(), default=[])
    utr_fallidas = ListField(StringField(), default=[])
    entidades_fallidas = ListField(StringField(), default=[])
    actualizado = DateTimeField(default=dt.datetime.now())
    ponderacion = FloatField(required=True, min_value=0, max_value=1, default=1)
    meta = {'allow_inheritance': True,'abstract':True}

    def __init__(self, *args, **values):
        super().__init__(*args, **values)
        if self.nombre is not None and self.tipo is not None:
            inicio = self.fecha_inicio.strftime('%d-%m-%Y %H:%M') if isinstance(self.fecha_inicio, dt.datetime) else self.fecha_inicio
            final = self.fecha_final.strftime('%d-%m-%Y %H:%M') if isinstance(self.fecha_final, dt.datetime) else self.fecha_final
            self.id_report = get_report_id(self.tipo, self.nombre, inicio, final)

    def __str__(self):
        return f"({self.tipo}, {self.nombre}):[ent:{len(self.reportes_entidades)}, tags:{self.numero_tags_total}]"

    def calculate_all(self):
        # en caso que la disponibilidad de una entidad sea -1, significa que no ha sido consignado en su totalidad
        # o que no se puede calcular ya que no tiene tags correctas para el cálculo:
        # para la ponderación solo se usarán aquellas que estan disponibles:
        numero_tags_total = sum([e.numero_tags for e in self.reportes_entidades
                                      if e.disponibilidad_promedio_ponderada_porcentage != -1])
        t_delta = self.fecha_final - self.fecha_inicio
        self.periodo_evaluacion_minutos = t_delta.days * (60 * 24) + t_delta.seconds // 60 + (t_delta.seconds % 60)/60
        self.disponibilidad_promedio_ponderada_porcentage = 0

        # si existen tags a considerar, el nodo no esta totalmente consignado
        if numero_tags_total > 0:
            for e in self.reportes_entidades:
                 if e.disponibilidad_promedio_ponderada_porcentage > 0:
                    e.ponderacion = e.numero_tags / numero_tags_total
                    self.disponibilidad_promedio_ponderada_porcentage += e.ponderacion * e.disponibilidad_promedio_ponderada_porcentage
            if self.disponibilidad_promedio_ponderada_porcentage > 100:
                self.disponibilidad_promedio_ponderada_porcentage = 100
            self.numero_tags_total = sum([e.numero_tags for e in self.reportes_entidades])

        # el nodo esta consignado totalmente, no se puede definir la disponibilidad:
        else:
            self.disponibilidad_promedio_ponderada_porcentage = -1
            # aunque el nodo este consignado totalmente, las tags en este nodo han sido consideradas
            self.numero_tags_total = sum([e.numero_tags for e in self.reportes_entidades])
            self.ponderacion = 0

        # ordenar los reportes por valor de disponibilidad
        self.reportes_entidades = sorted(self.reportes_entidades, key=lambda k: k["disponibilidad_promedio_ponderada_porcentage"])
        for ix, entidad in enumerate(self.reportes_entidades):
            reportes_utrs = sorted(entidad.reportes_instalaciones, key=lambda k: k["disponibilidad_promedio_porcentage"])
            self.reportes_entidades[ix].reportes_utrs = reportes_utrs

    def to_dict(self):
        return dict(id_node=str(self.nodo.id), id_report=self.id_report, tipo=self.tipo, nombre=self.nombre,
                    fecha_inicio=str(self.fecha_inicio),
                    fecha_final=str(self.fecha_final), actualizado=str(self.actualizado),
                    tiempo_calculo_segundos=self.tiempo_calculo_segundos,
                    periodo_evaluacion_minutos=self.periodo_evaluacion_minutos,
                    disponibilidad_promedio_ponderada_porcentage=self.disponibilidad_promedio_ponderada_porcentage,
                    tags_fallidas=self.tags_fallidas, tags_fallidas_detalle=self.tags_fallidas_detalle,
                    utr_fallidas=self.utr_fallidas,
                    entidades_fallidas=self.entidades_fallidas,
                    ponderacion=self.ponderacion,
                    numero_tags_total=self.numero_tags_total,
                    reportes_entidades=[r.to_dict() for r in self.reportes_entidades])

    def to_summary(self):
        novedades=dict(tags_fallidas=self.tags_fallidas, utr_fallidas=self.utr_fallidas,
                    entidades_fallidas=self.entidades_fallidas)
        n_entidades = len(self.reportes_entidades)
        n_rtus = sum([len(e.reportes_instalaciones) for e in self.reportes_entidades])
        procesamiento=dict(numero_tags_total=self.numero_tags_total, numero_utrs_procesadas=n_rtus,
                           numero_entidades_procesadas=n_entidades)
        return dict(id_report=self.id_report, nombre=self.nombre, tipo=self.tipo,
                    disponibilidad_promedio_ponderada_porcentage=self.disponibilidad_promedio_ponderada_porcentage,
                    novedades=novedades, procesamiento=procesamiento, actualizado=self.actualizado,
                    tiempo_calculo_segundos=self.tiempo_calculo_segundos)
