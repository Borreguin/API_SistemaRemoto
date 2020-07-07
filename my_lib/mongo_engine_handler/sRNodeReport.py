from my_lib.mongo_engine_handler.sRNode import *
import hashlib


class SRTagDetails(EmbeddedDocument):
    tag_name = StringField(required=True)
    indisponible_minutos = IntField(required=True)

    def __str__(self):
        return f"{self.tag_name}: {self.indisponible_minutos}"

    def to_dict(self):
        return dict(tag_name=self.tag_name, indisponible_minutos=self.indisponible_minutos)


class SRUTRDetails(EmbeddedDocument):
    id_utr = StringField(required=True)
    utr_nombre = StringField(required=True)
    utr_tipo = StringField(required=True)
    indisponibilidad_acumulada_minutos = IntField(required=True)
    indisponibilidad_detalle = ListField(EmbeddedDocumentField(SRTagDetails), required=True, default=list())
    consignaciones_acumuladas_minutos = IntField(required=True, default=0)
    consignaciones_detalle = ListField(EmbeddedDocumentField(Consignment))
    numero_tags = IntField(required=True)
    periodo_evaluacion_minutos = IntField(required=True)
    periodo_efectivo_minutos = IntField(required=True, default=0)
    # periodo_efectivo_minutos:
    # el periodo real a evaluar = periodo_evaluacion_minutos - consignaciones_acumuladas_minutos
    disponibilidad_promedio_minutos = FloatField(required=True, min_value=0, default=0)
    disponibilidad_promedio_porcentage = FloatField(required=True, min_value=0, max_value=100, default=0)
    ponderacion = FloatField(required=True, min_value=0, max_value=1, default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def calculate(self):
        self.numero_tags = len(self.indisponibilidad_detalle)
        self.indisponibilidad_acumulada_minutos = sum([t.indisponible_minutos for t in self.indisponibilidad_detalle])
        self.consignaciones_acumuladas_minutos = sum([c.t_minutos for c in self.consignaciones_detalle])
        if self.periodo_evaluacion_minutos is None and len(self.indisponibilidad_detalle) > 0:
            raise ValueError("Parámetro: 'periodo_efectivo_minutos' y 'indisponibilidad_detalle' son necesarios para "
                             "el cálculo")
        if self.periodo_evaluacion_minutos is not None and self.numero_tags > 0:
            # ordenar el reporte de tags:
            self.indisponibilidad_detalle = sorted(self.indisponibilidad_detalle,
                                                   key=lambda k:k['indisponible_minutos'],
                                                   reverse=True)
            self.periodo_efectivo_minutos = self.periodo_evaluacion_minutos - self.consignaciones_acumuladas_minutos
            self.disponibilidad_promedio_minutos = self.periodo_efectivo_minutos - \
                                                   (self.indisponibilidad_acumulada_minutos / self.numero_tags)
            if self.periodo_efectivo_minutos > 0:
                self.disponibilidad_promedio_porcentage = (self.disponibilidad_promedio_minutos
                                                           / self.periodo_efectivo_minutos) * 100

    def __str__(self):
        return f"{self.utr_nombre}: [{len(self.indisponibilidad_detalle)}] tags " \
               f"[{len(self.consignaciones_detalle)}] consig. " \
               f"(eval:{self.periodo_evaluacion_minutos} - cnsg:{self.consignaciones_acumuladas_minutos} = " \
               f" eftv:{self.periodo_efectivo_minutos} => disp_avg:{round(self.disponibilidad_promedio_minutos, 1)} " \
               f" %disp: {round(self.disponibilidad_promedio_porcentage, 2)})"

    def to_dict(self):
        return dict(id_utr=self.id_utr, nombre=self.utr_nombre, tipo=self.utr_tipo,
                    tag_details=[t.to_dict() for t in self.indisponibilidad_detalle],
                    numero_tags=len(self.indisponibilidad_detalle),
                    indisponibilidad_acumulada_minutos=self.indisponibilidad_acumulada_minutos,
                    consignaciones=[c.to_dict() for c in self.consignaciones_detalle],
                    consignaciones_acumuladas_minutos=self.consignaciones_acumuladas_minutos,
                    disponibilidad_promedio_porcentage=self.disponibilidad_promedio_porcentage,
                    ponderacion=self.ponderacion)


class SREntityDetails(EmbeddedDocument):
    entidad_nombre = StringField(required=True)
    entidad_tipo = StringField(required=True)
    reportes_utrs = ListField(EmbeddedDocumentField(SRUTRDetails), required=True, default=list())
    numero_tags = IntField(required=True, default=0)
    periodo_evaluacion_minutos = IntField(required=True)
    # el periodo real a evaluar = periodo_evaluacion_minutos - consignaciones_acumuladas_minutos
    disponibilidad_promedio_ponderada_minutos = FloatField(required=True, min_value=0, default=0)
    disponibilidad_promedio_ponderada_porcentage = FloatField(required=True, min_value=0, max_value=100, default=0)
    ponderacion = FloatField(required=True, min_value=0, max_value=1, default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def calculate(self):
        self.numero_tags = sum([u.numero_tags for u in self.reportes_utrs])
        if self.periodo_evaluacion_minutos is None and len(self.reportes_utrs) > 0:
            raise ValueError("Parámetro: 'periodo_evaluacion_minutos' y 'reportes_utrs' son necesarios para el cálculo")

        if self.numero_tags > 0:
            # calculo de las ponderaciones de cada UTR usando el número de tags como criterio
            for u in self.reportes_utrs:
                u.ponderacion = u.numero_tags / self.numero_tags

            self.disponibilidad_promedio_ponderada_porcentage = \
                sum([u.ponderacion * u.disponibilidad_promedio_porcentage for u in self.reportes_utrs])
            self.disponibilidad_promedio_ponderada_minutos = \
                sum([int(u.ponderacion * u.disponibilidad_promedio_minutos) for u in self.reportes_utrs])

    def __str__(self):
        return f"{self.entidad_tipo}:{self.entidad_nombre} [{len(self.reportes_utrs)}] utrs " \
               f"[{str(self.numero_tags)}] tags. " \
               f"(%disp_avg_pond:{round(self.disponibilidad_promedio_ponderada_porcentage, 3)} " \
               f" min_avg_pond:{round(self.disponibilidad_promedio_ponderada_minutos, 1)})"

    def to_dict(self):
        return dict(entidad_nombre=self.entidad_nombre, entidad_tipo=self.entidad_tipo, numero_tags=self.numero_tags,
                    reportes_utrs=[r.to_dict() for r in self.reportes_utrs],
                    disponibilidad_promedio_ponderada_porcentage=self.disponibilidad_promedio_ponderada_porcentage,
                    disponibilidad_promedio_ponderada_minutos=self.disponibilidad_promedio_ponderada_minutos,
                    periodo_evaluacion_minutos=self.periodo_evaluacion_minutos,
                    ponderacion=self.ponderacion)


class SRNodeDetails(Document):
    id_report = StringField(required=True, unique=True)
    nodo = LazyReferenceField(SRNode, required=True, passthrough=False)
    nombre = StringField(required=True, default=None)
    tipo = StringField(required=True, default=None)
    periodo_evaluacion_minutos = IntField(required=True)
    fecha_inicio = DateTimeField(required=True)
    fecha_final = DateTimeField(required=True)
    numero_tags_total = IntField(required=True)
    reportes_entidades = ListField(EmbeddedDocumentField(SREntityDetails), required=True, default=list())
    disponibilidad_promedio_ponderada_porcentage = FloatField(required=True, min_value=0, max_value=100)
    tiempo_calculo_segundos = FloatField(required=False)
    tags_fallidas = ListField(StringField(), default=[])
    utr_fallidas = ListField(StringField(), default=[])
    entidades_fallidas = ListField(StringField(), default=[])
    actualizado = DateTimeField(default=dt.datetime.now())
    ponderacion = FloatField(required=True, min_value=0, max_value=1, default=1)
    meta = {"collection": "REPORT|Nodos"}

    def __init__(self, *args, **values):
        super().__init__(*args, **values)
        if self.nombre is not None and self.tipo is not None:
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
                self.disponibilidad_promedio_ponderada_porcentage = \
                    sum([e.ponderacion * e.disponibilidad_promedio_ponderada_porcentage
                         for e in self.reportes_entidades])
        self.reportes_entidades = sorted(self.reportes_entidades, key=lambda k: k["disponibilidad_promedio_ponderada_porcentage"])
        for ix, entidad in enumerate(self.reportes_entidades):
            reportes_utrs = sorted(entidad.reportes_utrs, key=lambda k: k["disponibilidad_promedio_porcentage"])
            self.reportes_entidades[ix].reportes_utrs = reportes_utrs


    def to_dict(self):
        return dict(id_node=str(self.nodo.id), id_report=self.id_report, tipo=self.tipo, nombre=self.nombre,
                    fecha_inicio=str(self.fecha_inicio),
                    fecha_final=str(self.fecha_final), actualizado=str(self.actualizado),
                    tiempo_calculo_segundos=self.tiempo_calculo_segundos,
                    periodo_evaluacion_minutos=self.periodo_evaluacion_minutos,
                    disponibilidad_promedio_ponderada_porcentage=self.disponibilidad_promedio_ponderada_porcentage,
                    tags_fallidas=self.tags_fallidas, utr_fallidas=self.utr_fallidas,
                    entidades_fallidas=self.entidades_fallidas,
                    ponderacion=self.ponderacion,
                    numero_tags_total=self.numero_tags_total,
                    reportes_entidades=[r.to_dict() for r in self.reportes_entidades])

    def to_summary(self):
        novedades=dict(tags_fallidas=self.tags_fallidas, utr_fallidas=self.utr_fallidas,
                    entidades_fallidas=self.entidades_fallidas)
        n_entidades = len(self.reportes_entidades)
        n_rtus = sum([len(e.reportes_utrs) for e in self.reportes_entidades])
        procesamiento=dict(numero_tags_total=self.numero_tags_total, numero_utrs_procesadas=n_rtus,
                           numero_entidades_procesadas=n_entidades)
        return dict(id_report=self.id_report, nombre=self.nombre, tipo=self.tipo,
                    disponibilidad_promedio_ponderada_porcentage=self.disponibilidad_promedio_ponderada_porcentage,
                    novedades=novedades, procesamiento=procesamiento, actualizado=self.actualizado,
                    tiempo_calculo_segundos=self.tiempo_calculo_segundos)
