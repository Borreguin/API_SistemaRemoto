from my_lib.mongo_engine_handler.sRNode import *
import hashlib


class SRTagDetails(EmbeddedDocument):
    tag_name = StringField(required=True)
    indisponible_minutos = IntField(required=True)

    def __str__(self):
        return f"{self.tag_name}: {self.indisponible_minutos}"

    def to_dict(self):
        return dict(tag_name=self.tag_name, indisponible_minutos=self.indisponible_minutos)


class SREntityDetails(EmbeddedDocument):
    id_entidad = StringField(required=True)
    nombre = StringField(required=True)
    tipo = StringField(required=True)
    indisponibilidad_acumulada_minutos = IntField(required=True)
    indisponibilidad_detalle = ListField(EmbeddedDocumentField(SRTagDetails), required=True)
    consignaciones_acumuladas_minutos = IntField(required=True, default=0)
    consignaciones_detalle = ListField(EmbeddedDocumentField(Consignment))
    numero_tags = IntField(required=True)
    periodo_evaluacion_minutos = IntField(required=True)
    periodo_efectivo_minutos = IntField(required=True)
    # periodo_efectivo_minutos:
    # el periodo real a evaluar = periodo_evaluacion_minutos - consignaciones_acumuladas_minutos
    disponibilidad_promedio_minutos = FloatField(required=True, min_value=0)
    disponibilidad_promedio_porcentage = FloatField(required=True, min_value=0, max_value=100, default=0)
    ponderacion = FloatField(required=True, min_value=0, max_value=1, default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calculate()

    def calculate(self):
        self.numero_tags = len(self.indisponibilidad_detalle)
        self.indisponibilidad_acumulada_minutos = sum([t.indisponible_minutos for t in self.indisponibilidad_detalle])
        self.consignaciones_acumuladas_minutos = sum([c.t_minutos for c in self.consignaciones_detalle])
        if self.periodo_evaluacion_minutos is None and len(self.indisponibilidad_detalle) > 0:
            raise ValueError("Parámetro: 'periodo_efectivo_minutos' y 'indisponibilidad_detalle' son necesarios para el cálculo")
        if self.periodo_evaluacion_minutos is not None and self.numero_tags > 0:
            self.periodo_efectivo_minutos = self.periodo_evaluacion_minutos - self.consignaciones_acumuladas_minutos
            self.disponibilidad_promedio_minutos = self.periodo_efectivo_minutos - \
                                               (self.indisponibilidad_acumulada_minutos/self.numero_tags)
            if self.periodo_efectivo_minutos > 0:
                self.disponibilidad_promedio_porcentage = (self.disponibilidad_promedio_minutos/self.periodo_efectivo_minutos)*100

    def __str__(self):
        return f"{self.nombre}: [{len(self.indisponibilidad_detalle)}] tags " \
               f"[{len(self.consignaciones_detalle)}] consig. " \
               f"(eval:{self.periodo_evaluacion_minutos} - cnsg:{self.consignaciones_acumuladas_minutos} = " \
               f" eftv:{self.periodo_efectivo_minutos} => disp_avg:{round(self.disponibilidad_promedio_minutos,1)} " \
               f" %disp: {round(self.disponibilidad_promedio_porcentage, 2)})"

    def to_dict(self):
        return dict(id_entidad=self.id_entidad, nombre=self.nombre, tipo=self.tipo,
                    tag_details=[t.to_dict() for t in self.indisponibilidad_detalle], numero_tags=len(self.indisponibilidad_detalle),
                    indisponibilidad_acumulada_minutos=self.indisponibilidad_acumulada_minutos,
                    consignaciones=[c.to_dict() for c in self.consignaciones_detalle],
                    consignaciones_acumuladas_minutos=self.consignaciones_acumuladas_minutos,
                    ponderacion=self.ponderacion)


class SRNodeDetails(Document):
    id_report = StringField(required=True, unique=True)
    nodo = LazyReferenceField(SRNode, required=True, passthrough=True)
    nombre = StringField(required=True)
    tipo = StringField(required=True)
    periodo_evaluacion_minutos = IntField(required=True)
    fecha_inicio = DateTimeField(required=True)
    fecha_final = DateTimeField(required=True)
    numero_tags_total = IntField(required=True)
    reportes_entidades = ListField(EmbeddedDocumentField(SREntityDetails), required=True)
    disponibilidad_promedio_ponderada_porcentage = FloatField(required=True, min_value=0, max_value=100)
    tiempo_calculo_segundos = FloatField(required=False)
    tags_fallidas = ListField(StringField(), default=[])
    entidades_fallidas = ListField(StringField(), default=[])
    meta = {"collection": "REPORT|Nodos"}

    def __init__(self, *args, **values):
        super().__init__(*args, **values)
        if self.nodo is not None:
            self.nombre = self.nodo.nombre
            self.tipo = self.nodo.tipo
            id = str(self.nombre).lower().strip() + str(self.tipo).lower().strip() \
                 + self.fecha_inicio.strftime('%d-%m-%Y %H:%M') + self.fecha_final.strftime('%d-%m-%Y %H:%M')
            self.id_report = hashlib.md5(id.encode()).hexdigest()
        self.calculate_all()


    def calculate_all(self):
        self.numero_tags_total = sum([e.numero_tags for e in self.reportes_entidades])
        t_delta = self.fecha_final - self.fecha_inicio
        self.periodo_evaluacion_minutos = t_delta.days * (60 * 24) + t_delta.seconds // 60 + t_delta.seconds % 60
        for e in self.reportes_entidades:
            if self.numero_tags_total > 0:
                e.ponderacion = e.numero_tags / self.numero_tags_total
        self.disponibilidad_promedio_ponderada_porcentage = sum([e.ponderacion * e.disponibilidad_promedio_porcentage
                                                                 for e in self.reportes_entidades])