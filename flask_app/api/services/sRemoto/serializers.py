from flask_restplus import fields

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

        self.name_update = api.inherit("Permite actualizar Nombre", {
            "nuevo_nombre": fields.String(description="Renombrar elemento",
                                          required=True, default="Nuevo nombre")})

        self.name_delete = api.model("Eliminar Elemento", {
            "eliminar_elemento": fields.String(description="Nombre del elemento a eliminar",
                                               required=True, default="Elemento a eliminar")})

        self.tagname = api.model("Configuración Tagname", {
            "tag_name": fields.String(required=True, description="Nombre de tag"),
            "filter_expression": fields.String(required=True, description="Expresión de filtro indisponibilidad"),
            "activado": fields.Boolean(required=False, default=True, description="Activación de tag")})

        self.list_tagname = api.model("Lista Tagname", {
            "tags": fields.List(fields.Nested(self.tagname))
        })

        self.edited_tagname = api.model("Editar una STR Tag", {
            "tag_name": fields.String(required=True, description="Nombre editado de la tag, es el nuevo nombre de la tag"),
            "filter_expression": fields.String(required=True, description="Expresión de filtro indisponibilidad"),
            "activado": fields.Boolean(required=False, default=True, description="Activación de tag"),
            "tag_name_original": fields.String(required=True, description="Nombre original de la tag, es el nombre de Tag a editar"),
        })

        self.list_edited_tagname = api.model("Lista de STR Tag a editar", {
            "tags": fields.List(fields.Nested(self.edited_tagname))
        })

        self.tags = api.model("Lista nombre tags", {
            "tags": fields.List(fields.String())
        })

        self.rtu = api.model("Configurar RTU",
                                 {
                                     "id_utr": fields.String(required=True,
                                                             description="Identificación única de la RTU. Ex: TBOX TEST DNP3"),
                                     "utr_tipo": fields.String(required=True,
                                                                     description="Tipo de RTU: Subestación, Central, etc"),
                                     "utr_nombre": fields.String(required=True,
                                                                   description="Nombre de la RTU "),
                                     "activado": fields.Boolean(default=True,
                                                                description="Activación de la entity_list"),
                                     "protocol": fields.String(default="No definido", required=True,
                                                                description="Definición de protocolo de la UTR"),
                                     "latitude": fields.Float(default=0, required=True,
                                                               description="Georeferencia Latitud de la UTR"),
                                     "longitude": fields.Float(default=0, required=True,
                                                              description="Georeferencia Longitud de la UTR"),
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

        self.nodo = api.model("Nuevo Nodo", {
            "tipo": fields.String(required=True, description="Tipo de nodo"),
            "nombre": fields.String(required=True, description="Nombre de nodo"),
            "activado": fields.Boolean(required=False, default=True, description="Activacion Nodo")
        })


        self.nodos = api.model("Lista de tipos y nombres de nodos", {
            "nodos": fields.List(fields.Nested(self.nodo), default=[dict(tipo="Empresa", nombre="Nombre")])
        })

        self.nodes = api.model("Lista nombre de nodos", {
            "nodos": fields.List(fields.String(), default=["nodo1", "nodo2", "etc"])
        })

        self.rtu_id = api.model("UTR id", {
            "id_utr": fields.String(required=True, description="Identificación única. Ex: UTR TEST DNP")
        })
        return api
