from settings.initial_settings import SUPPORTED_FORMAT_DATES as time_formats
from flask_restplus import fields, Model

import datetime as dt

"""
    Configure the API HTML to show for each services the schemas that are needed 
    for posting and putting
    (Explain the arguments for each service)
    Los serializadores explican los modelos (esquemas) esperados por cada servicio
"""


class ConsignacionSerializers:

    def __init__(self, app):
        self.api = app

    def add_serializers(self):
        api = self.api

        """ Serializador para formulario """
        self.detalle_consignacion = api.model("Detalles de consignación",
                               {
                                   "no_consignacion": fields.String(required=True,
                                                             description="Id de elemento"),
                                   "detalle": fields.Raw(required=False,
                                                       description="json con detalle de la consignación")
                               })
        return api
