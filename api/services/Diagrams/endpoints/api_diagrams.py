

from flask_restplus import Resource
from api.services.restplus_config import default_error_handler

# importando configuraciones iniciales
from settings import initial_settings as init
from api.services.restplus_config import api
from my_lib.mongo_engine_handler.DiagramSerializer import *

# configurando logger y el servicio web
log = init.LogDefaultConfig("ws_sRemoto.log").logger
ns = api.namespace('ui-diagrams', description='Relativas a los diagramas de la interfaz gráfica')


@ns.route('/diagram/<string:tipo>/<string:nombre>')
class DiagramSerializerAPI(Resource):

    def get(self, tipo:str="tipo del diagrama serializado", nombre:str="Nombre del diagrama serializado"):
        """ Busca un diagrama serializado """
        try:
            query = Q(nombre=nombre) and Q(tipo=tipo)
            diagram = DiagramSerializer.objects(query).first()
            if diagram is None:
                return dict(nombre=nombre, tipo=tipo, diagrama=dict()), 404
            return diagram.to_dict(), 200
        except Exception as e:
            return default_error_handler(e)