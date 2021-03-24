from settings.initial_settings import SUPPORTED_FORMAT_DATES as time_formats
from flask_restplus import fields, Model

import datetime as dt

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

        self.time = api.model("Hora", {
            "hour": fields.Integer(required=False, default=0),
            "minute": fields.Integer(required=False, default=0),
            "second": fields.Integer(required=False, default=0)
        })

        """ serializador configuración de reportes """
        self.report_config = api.model("Configuración de reportes", {
            "span": fields.Nested(self.span, description="Rango de tiempo entre reportes"),
            "trigger": fields.Nested(self.time, description="Hora de ejecución"),
            "send_mail": fields.Nested(self.time, description="Envío de mail")})

        return api
