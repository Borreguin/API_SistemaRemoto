from mongoengine import EmbeddedDocument, StringField, ListField, EmbeddedDocumentField, IntField, FloatField

from app.db.v1.SRNodeReport.sRNodeReportBase import SRUTRDetails


class V2SREntityDetails(EmbeddedDocument):
    entidad_nombre = StringField(required=True)
    entidad_tipo = StringField(required=True)
    reportes_installations = ListField(EmbeddedDocumentField(SRUTRDetails), required=True, default=list())
    numero_tags = IntField(required=True, default=0)
    periodo_evaluacion_minutos = IntField(required=True)
    # el periodo real a evaluar = periodo_evaluacion_minutos - consignaciones_acumuladas_minutos
    # se permite el valor de -1 en caso que sea indefinida la disponibilidad:
    # esto ocurre por ejemplo en el caso que la totalidad del periodo evaluado está consignado
    disponibilidad_promedio_ponderada_minutos = FloatField(required=True, min_value=-1, default=0)
    disponibilidad_promedio_ponderada_porcentage = FloatField(required=True, min_value=-1, max_value=100, default=0)
    ponderacion = FloatField(required=True, min_value=0, max_value=1, default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def calculate(self):
        if self.periodo_evaluacion_minutos is None and len(self.reportes_installations) > 0:
            raise ValueError("Parámetro: 'periodo_evaluacion_minutos' y 'reportes_installations' son necesarios para el cálculo")
        # considerando el caso donde hay consignaciones que abarcan la totalidad del periodo
        #  en el que se evalua la consignación. En estos casos la disponibilidad es -1
        # y su periodo efectivo en minutos es mayor a cero
        self.numero_tags = sum([u.numero_tags for u in self.reportes_installations if u.periodo_efectivo_minutos > 0])
        if self.numero_tags > 0:
            # calculo de las ponderaciones de cada UTR usando el número de tags como criterio
            for u in self.reportes_installations:
                if u.periodo_efectivo_minutos > 0:
                    # caso normal cuando existe un tiempo efectivo a evaluar
                    u.ponderacion = u.numero_tags / self.numero_tags
                else:
                    # caso cuando está consignado totalmente, en ese caso no es tomado en cuenta
                    u.ponderacion = 0

            self.disponibilidad_promedio_ponderada_porcentage = \
                sum([u.ponderacion * u.disponibilidad_promedio_porcentage for u in self.reportes_installations])
            self.disponibilidad_promedio_ponderada_minutos = \
                sum([int(u.ponderacion * u.disponibilidad_promedio_minutos) for u in self.reportes_installations])
        else:
            # si no hay tags, no se puede definir la disponibilidad de la entidad por lo que su valor es -1
            self.disponibilidad_promedio_ponderada_porcentage = -1
            self.disponibilidad_promedio_ponderada_minutos = -1
            # estas son las tags evaluadas a pesar de que el periodo esta totalmente consignado
            self.numero_tags = sum([u.numero_tags for u in self.reportes_installations])
            # no puede tener ponderación ya que no tiene valores de disponibilidad válidos
            self.ponderacion = 0

    def __str__(self):
        return f"{self.entidad_tipo}:{self.entidad_nombre} [{len(self.reportes_installations)}] utrs " \
               f"[{str(self.numero_tags)}] tags. " \
               f"(%disp_avg_pond:{round(self.disponibilidad_promedio_ponderada_porcentage, 3)} " \
               f" min_avg_pond:{round(self.disponibilidad_promedio_ponderada_minutos, 1)})"

    def to_dict(self):
        return dict(entidad_nombre=self.entidad_nombre, entidad_tipo=self.entidad_tipo, numero_tags=self.numero_tags,
                    reportes_utrs=[r.to_dict() for r in self.reportes_installations],
                    disponibilidad_promedio_ponderada_porcentage=self.disponibilidad_promedio_ponderada_porcentage,
                    disponibilidad_promedio_ponderada_minutos=self.disponibilidad_promedio_ponderada_minutos,
                    periodo_evaluacion_minutos=self.periodo_evaluacion_minutos,
                    ponderacion=self.ponderacion)