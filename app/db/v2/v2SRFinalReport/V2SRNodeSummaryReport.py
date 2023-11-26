from mongoengine import EmbeddedDocument, StringField, FloatField, DictField, DateTimeField
import datetime as dt

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