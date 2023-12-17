from mongoengine import EmbeddedDocument, StringField, IntField, ListField, EmbeddedDocumentField, FloatField

from app.db.v2.entities.v2_sRInstallation import V2SRInstallation
from app.db.v2.v2SRNodeReport.Details.v2_sRBahiaReportDetails import V2SRBahiaReportDetails


class V2SRInstallationReportDetails(EmbeddedDocument):
    document_id = StringField(required=True)
    instalacion_id = StringField(required=True)
    instalacion_ems_code=StringField(required=True)
    instalacion_nombre = StringField(required=True)
    instalacion_tipo = StringField(required=True)
    indisponibilidad_acumulada_minutos = IntField(required=True)
    reportes_bahias = ListField(EmbeddedDocumentField(V2SRBahiaReportDetails), required=False, default=list())
    consignaciones_acumuladas_minutos = IntField(required=True, default=0)
    numero_tags = IntField(required=True)
    periodo_evaluacion_minutos = IntField(required=True)
    periodo_efectivo_minutos = IntField(required=True, default=0)
    # periodo_efectivo_minutos:
    # el periodo real a evaluar = periodo_evaluacion_minutos - consignaciones_acumuladas_minutos
    # se admite el valor de -1 para los casos en los que la disponibilidad queda indefinida
    # Ej: Cuando el periodo evaluado está consignado en su totalidad
    disponibilidad_promedio_minutos = FloatField(required=True, min_value=-1, default=0)
    disponibilidad_promedio_porcentage = FloatField(required=True, min_value=-1, max_value=100, default=0)
    ponderacion = FloatField(required=True, min_value=0, max_value=1, default=1)

    def __init__(self, installation: V2SRInstallation, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.instalacion_id = installation.instalacion_id
        self.instalacion_ems_code = installation.instalacion_ems_code
        self.instalacion_nombre = installation.instalacion_nombre
        self.instalacion_tipo = installation.instalacion_tipo
        self.document_id = installation.get_document_id()


    def __str__(self):
        return f"{self.instalacion_nombre}: [{len(self.reportes_bahias)}] tags " \
               f"(eval:{self.periodo_evaluacion_minutos} - cnsg:{self.consignaciones_acumuladas_minutos} = " \
               f" eftv:{self.periodo_efectivo_minutos} => disp_avg:{round(self.disponibilidad_promedio_minutos, 1)} " \
               f" %disp: {round(self.disponibilidad_promedio_porcentage, 2)})"

    def calculate(self, report_ini_date, report_end_date):
        pass
        # self.numero_tags = len(self.reportes_bahias)
        # self.indisponibilidad_acumulada_minutos = sum([t.indisponible_minutos for t in self.reportes_bahias])
        # self.consignaciones_acumuladas_minutos = 0
        # # las consignaciones se enmarcan dentro de un periodo de reporte
        # # si alguna consignación sale del periodo, entonces debe ser acotada al periodo:
        # for consignacion in self.consignaciones_detalle:
        #     temp_consignacion = consignacion
        #     if temp_consignacion.fecha_inicio < report_ini_date:
        #         temp_consignacion.fecha_inicio = report_ini_date
        #     if temp_consignacion.fecha_final > report_end_date:
        #         temp_consignacion.fecha_final = report_end_date
        #     temp_consignacion.calculate()
        #     self.consignaciones_acumuladas_minutos += temp_consignacion.t_minutos
        # if self.periodo_evaluacion_minutos is None and len(self.reportes_bahias) > 0:
        #     raise ValueError("Parámetro: 'periodo_efectivo_minutos' y 'indisponibilidad_detalle' son necesarios para "
        #                      "el cálculo")
        # if self.periodo_evaluacion_minutos is not None and self.numero_tags > 0:
        #     # ordenar el reporte de tags:
        #     self.reportes_bahias = sorted(self.reportes_bahias,
        #                                   key=lambda k:k['indisponible_minutos'],
        #                                   reverse=True)
        #     self.periodo_efectivo_minutos = self.periodo_evaluacion_minutos - self.consignaciones_acumuladas_minutos
        #
        #     if self.periodo_efectivo_minutos > 0:
        #         self.disponibilidad_promedio_minutos = self.periodo_efectivo_minutos - \
        #                                                (self.indisponibilidad_acumulada_minutos / self.numero_tags)
        #         self.disponibilidad_promedio_porcentage = (self.disponibilidad_promedio_minutos
        #                                                    / self.periodo_efectivo_minutos) * 100
        #         assert self.disponibilidad_promedio_porcentage <= 100
        #         assert self.disponibilidad_promedio_porcentage >= -1
        #     # este caso ocurre cuando la totalidad del periodo está consignado:
        #     else:
        #         self.disponibilidad_promedio_minutos = -1
        #         self.disponibilidad_promedio_porcentage = -1
        # else:
        #     # en caso de no tener tags válidas
        #     self.disponibilidad_promedio_minutos = -1
        #     self.disponibilidad_promedio_porcentage = -1


    def to_dict(self):
        return dict(id_utr=self.instalacion_id, nombre=self.instalacion_nombre, tipo=self.instalacion_tipo,
                    tag_details=[t.to_dict() for t in self.reportes_bahias],
                    numero_tags=len(self.reportes_bahias),
                    indisponibilidad_acumulada_minutos=self.indisponibilidad_acumulada_minutos,
                    consignaciones_acumuladas_minutos=self.consignaciones_acumuladas_minutos,
                    disponibilidad_promedio_porcentage=self.disponibilidad_promedio_porcentage,
                    ponderacion=self.ponderacion)
