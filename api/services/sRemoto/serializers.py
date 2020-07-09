from settings.initial_settings import SUPPORTED_FORMAT_DATES as time_formats
from flask_restplus import fields, Model

import datetime as dt

"""
    Configure the API HTML to show for each services the schemas that are needed 
    for posting and putting
    (Explain the arguments for each service)
    Los serializadores explican los modelos (esquemas) esperados por cada servicio
"""


class sRemotoSerializers:

    def __init__(self, app):
        self.api = app

    def add_serializers(self):
        api = self.api

        """ serializador para nodos """
        self.nombre = api.model('Nombre', {"nombre": fields.String(required=True, description="Nombre del nodo")})

        self.name_update = api.inherit("Actualizar Nombre", {
            "nuevo_nombre": fields.String(description="Renombrar elemento",
                                          required=True, default="Nuevo nombre")})

        self.name_delete = api.model("Eliminar Elemento", {
            "eliminar_elemento": fields.String(description="Nombre del elemento a eliminar",
                                               required=True, default="Elemento a eliminar")})

        self.tagname = api.model("Configuración Tagname", {
            "tag_name": fields.String(required=True, description="Nombre de tag"),
            "filter_expression": fields.String(required=True, description="Expresión de filtro indisponibilidad"),
            "activado": fields.Boolean(default=True, description="Activación de tag")})

        self.list_tagname = api.model("Lista Tagname", {
            "tags": fields.List(fields.Nested(self.tagname))
        })

        self.tags = api.model("Lista nombre tags", {
            "tags": fields.List(fields.String())
        })

        self.entidad = api.model("Configurar Entidad",
                {
                    "id_utr": fields.String(required=True, description="Identificación única. Ex: Nombre UTR"),
                    "entidad_nombre": fields.String(required=True, description="Nombre de la entity_list"),
                    "entidad_tipo": fields.String(required=True, description="Tipo de entity_list: Unidad de Negocio, Empresa, etc"),
                    "tags": fields.List(fields.Nested(self.tagname)),
                    "activado": fields.Boolean(default=True, description="Activación de la entity_list")
                })

        self.node = api.model("Configurar Nodo",{
            "nombre": fields.String(required=True, description="Nombre del nodo"),
            "tipo": fields.String(required=True, description="Tipo de nodo. Ex: , Empresa, Subdivisión, etc."),
            "actualizado": fields.DateTime(default=dt.datetime.now()),
            "entidades": fields.List(fields.Nested(self.entidad)),
            "activado": fields.Boolean(default=True, description="Activación del nodo")
        })

        self.nodo = api.model("Nodo", {
            "tipo": fields.String(required=True, description="Tipo de nodo"),
            "nombre": fields.String(required=True, description="Nombre de nodo")
        })

        self.nodos = api.model("Lista de tipos y nombres de nodos", {
            "nodos": fields.List(fields.Nested(self.nodo), default=[dict(tipo="Empresa", nombre="Nombre")])
        })

        self.nodes = api.model("Lista nombre de nodos", {
            "nodos": fields.List(fields.String(), default=["nodo1", "nodo2", "etc"])
        })
        return api
