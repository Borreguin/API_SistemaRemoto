from flask_restplus import fields

"""
    Configure the API HTML to show for each services the schemas that are needed 
    for posting and putting
    (Explain the arguments for each service)
    Los serializadores explican los modelos (esquemas) esperados por cada servicio
"""


class Serializers:

    def __init__(self, app):
        self.api = app

    def add_serializers(self):
        api = self.api

        """ serializador configuración de reportes """
        self.span = api.model("Span de tiempo", {
            "days": fields.Float(required=False, default=0),
            "hours": fields.Float(required=False, default=0),
            "minutes": fields.Float(required=False, default=0),
            "seconds": fields.Float(required=False, default=0)
        })

        self.trigger = api.model("Trigger", {
            "hours": fields.Float(required=False, default=0),
            "minutes": fields.Float(required=False, default=0),
            "seconds": fields.Integer(required=False, default=0)
        })

        self.mail_config = api.model("Configuración mail", {
            "from_email": fields.String(required=True, default="sistemaremoto@cenace.org.ec"),
            "users": fields.List(fields.String, required=True, default=[]),
            "admin": fields.List(fields.String, required=True, default=[]),
        })

        self.parameters = api.model("Parámetros Umbrales", {
            "disp_utr_umbral": fields.Float(required=False, default=0.95),
            "disp_tag_umbral": fields.Float(required=False, default=0.95)
        })

        """ serializador para configuración de ejecución de reportes """
        self.report_config = api.model("Configuración del reporte", {
            "trigger": fields.Nested(self.trigger, description="Hora de ejecución del reporte", required=True),
            "mail_config": fields.Nested(self.mail_config, description="Configuración mail", required=False),
            "parameters": fields.Nested(self.parameters, description="Configuración de parámetros", required=False),
        })

        return api
