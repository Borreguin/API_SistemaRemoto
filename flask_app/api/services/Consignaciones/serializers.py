from flask_restplus import fields

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

        """ Serializador para formulario reducido de consignacion """
        self.detalle_consignacion = api.model("Detalles menores de consignación",
                               {
                                   "elemento": fields.Raw(required=False,
                                                          description="Descripción del elemento en formato JSON"),
                                   "no_consignacion": fields.String(required=True,
                                                             description="Id de elemento"),
                                   "detalle": fields.Raw(required=False,
                                                       description="json con detalle de la consignación")
                               })

        """ Serializador para formulario extendido de consignacion"""
        self.consignacion = api.model("Detalles de consignación",
                                              {
                                                  "no_consignacion": fields.String(required=True,
                                                                                   description="Id de elemento"),
                                                  "fecha_inicio": fields.String(required=True,
                                                                                description="formato: [yyyy-mm-dd hh:mm:ss]"),
                                                  "fecha_final": fields.String(required=True,
                                                                                description="formato: [yyyy-mm-dd hh:mm:ss]"),
                                                  "detalle": fields.Raw(required=False,
                                                                        description="json con detalle de la consignación")
                                              })

        return api
