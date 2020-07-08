# Created by Roberto Sanchez at 4/16/2019
# -*- coding: utf-8 -*-
""""
    Servicio Web de Sistema Remoto:
        - Permite la administración de Nodos, Entidades y Tags
        - Serializar e ingresar datos

    If you need more information. Please contact the email above: rg.sanchez.arg_from@gmail.com
    "My work is well done to honor God at any time" R Sanchez A.
    Mateo 6:33
"""

from flask_restplus import Resource
from flask import request
import re, os
# importando configuraciones iniciales
from settings import initial_settings as init
from api.services.restplus_config import api
from api.services.restplus_config import default_error_handler
from api.services.sRemoto import serializers as srl
from api.services.sRemoto import parsers
# importando clases para leer desde MongoDB
from my_lib.mongo_engine_handler.sRNode import *
from random import randint

# configurando logger y el servicio web
log = init.LogDefaultConfig("ws_sRemoto.log").logger
ns = api.namespace('admin-sRemoto', description='Relativas a la administración de nodos de Sistema Remoto')

ser_from = srl.sRemotoSerializers(api)
api = ser_from.add_serializers()


@ns.route('/nodo/<string:nombre>')
class SRNodeAPI(Resource):

    def get(self, nombre: str = "Nombre del nodo a buscar"):
        """ Busca si un nodo tipo SRNode existe en base de datos """
        try:
            nodo = SRNode.objects(nombre=nombre).first()
            if nodo is None:
                return nodo, 404
            return nodo.to_dict(), 200
        except Exception as e:
            return default_error_handler(e)

    @api.expect(ser_from.name_update)
    def put(self, nombre: str = "Nombre del nodo a cambiar"):
        """
        Actualiza el nombre de un nodo
        """
        new_name = request.json["nuevo_nombre"]
        try:
            nodo = SRNode.objects(nombre=nombre).first()
            if nodo is None:
                return nodo, 404
            nodo.nombre = new_name
            nodo.actualizado = dt.datetime.now()
            nodo.save()
            return nodo.to_dict(), 200
        except Exception as e:
            return default_error_handler(e)


@ns.route('/nodo/id/<string:id>')
class SRNodeIDAPI(Resource):
    def delete(self, id):
        """ Elimina nodo usando su ID como referencia """
        try:
            nodo = SRNode.objects(id_node=id).first()
            if nodo is None:
                return dict(success=False, errors="No se encontró el nodo"), 404
            nodo.delete()
            return nodo.to_dict(), 200
        except Exception as e:
            return default_error_handler(e)

    def put(self, id):
        """ Actualiza cambios menores en el nodo (nombres, tipos, activaciones) """
        try:
            request_data = dict(request.json)
            id_node = request_data["id_node"]
            node = SRNode.objects(id_node=id_node).first()
            if node is None:
                return dict(success=False, errors="No se encontró el nodo"), 404
            success, msg = node.update_summary_info(request_data)
            if not success:
                return dict(success=False, errors=msg), 400
            node.save()
            return node.to_summary(), 200
        except Exception as e:
            return default_error_handler(e)

    def post(self, id):
        """ Crea un nuevo nodo usando ID """
        try:
            request_data = dict(request.json)
            nodo = SRNode.objects(id_node=id).first()
            if nodo is not None:
                return dict(success=False, errors="El nodo ya existe, no puede ser creado"), 400
            nodo = SRNode(nombre=request_data["nombre"], tipo=request_data["tipo"], activado=request_data["activado"])
            success, msg = nodo.update_summary_info(request_data)
            if not success:
                return dict(success=False, errors=msg), 400
            nodo.save()
            return nodo.to_summary(), 200
        except Exception as e:
            return default_error_handler(e)


@ns.route('/nodo')
class PostSRNodeAPI(Resource):
    @api.expect(ser_from.node)
    @api.response(409, 'No es posible crear este nodo')
    def post(self):
        """ Crear un SRNode """
        try:
            request_data = dict(request.json)
            nodo = SRNode(**request_data)
            nodo.save()
            return nodo.to_dict(), 200
        except Exception as e:
            return default_error_handler(e)


@ns.route('/nodo/<string:nombre>/entidad')
class SREntidadAPI(Resource):
    @api.expect(ser_from.entidad)
    @api.response(404, 'No se puede añadir. El nodo especificado en "nombre" no existe')
    def put(self, nombre):
        """ Añadir/substituir una entidad en el nodo "nombre"
            Si el nodo no existe entonces error 404
            La entidad es añadida a un nodo ya existente
            Si la entidad ya existe entonces es reemplazada
        """
        try:
            nodo = SRNode.objects(nombre=nombre).first()
            if nodo is None:
                return nodo, 404
            request_data = dict(request.json)
            entidad = SREntity(**request_data)
            nodo.add_or_replace_entities(entidad)
            nodo.save()
            return nodo.to_dict(), 200
        except Exception as e:
            return default_error_handler(e)

    @api.response(404, 'No se encuentran resultados')
    @api.expect(ser_from.name_delete)
    def delete(self, nombre):
        """ Eliminar una entidad en el nodo "nombre"
            Si el nodo no existe entonces error 404
            Si la entidad no existe entonces error 404
        """
        try:
            nodo = SRNode.objects(nombre=nombre).first()
            if nodo is None:
                return nodo, 404
            name_delete = dict(request.json)["eliminar_elemento"]
            success, msg = nodo.delete_entity(name_delete)
            if success:
                nodo.save()
                return nodo.to_dict(), 200
            else:
                return dict(success=success, errors=msg), 404
        except Exception as e:
            return default_error_handler(e)


@ns.route('/nodo/<string:nombre>/<string:entidad>')
class SREntidadesAPI1(Resource):
    @api.response(404, 'No se encuentran resultados')
    def get(self, nombre, entidad):
        """ Muestra la entidad
            Si el nodo no existe entonces error 404
            Si la entidad no existe entonces error 404
        """
        try:
            nodo = SRNode.objects(nombre=nombre).first()
            if nodo is None:
                return nodo, 404
            success, result = nodo.search_entity(entidad)
            return (result.to_dict(), 200) if success else (dict(success=success, errors=result), 404)
        except Exception as e:
            return default_error_handler(e)


@ns.route('/nodo/<string:nombre>/entidades')
class SREntidadesAPI2(Resource):
    @api.response(404, 'No se encuentran resultados')
    def get(self, nombre):
        """ Muestra las entidades de un nodo
            Si nodo no existe entonces error 404
        """
        try:
            nodo = SRNode.objects(nombre=nombre).first()
            if nodo is None:
                return nodo, 404
            return [e.to_dict() for e in nodo.entidades], 200
        except Exception as e:
            return default_error_handler(e)


@ns.route('/nodo/<string:nombre>/<string:entidad>/tags')
class SRTagsAPI(Resource):
    @api.expect(ser_from.list_tagname)
    def put(self, nombre, entidad):
        """ Actualiza la configuración de tags en una entidad
            Si el nodo no existe entoces error 404
            Si entidad no existe entonces error 404
        """
        try:
            tags = dict(request.json)["tags"]
            tag_list = [SRTag(**t) for t in tags]
            nodo = SRNode.objects(nombre=nombre).first()
            if nodo is None:
                return dict(success=False, errors=f"No existe nodo [{nombre}]"), 404
            success, msg = nodo.add_or_replace_tags_in_entity(tag_list, entidad)
            if success:
                nodo.save()
                correct, r = nodo.search_entity(entidad)
                return (r.to_dict(), 200) if correct else (r, 400)
            return dict(success=False, errors=msg), 400
        except Exception as e:
            return default_error_handler(e)

    def get(self, nombre, entidad):
        """ Muestra las tags dentro de una entidad """
        try:
            nodo = SRNode.objects(nombre=nombre).first()
            if nodo is None:
                return dict(success=False, errors=f"No existe nodo [{nombre}]"), 404
            check = [i for i, e in enumerate(nodo.entidades) if entidad == e.nombre]
            if len(check) > 0:
                ix = check[0]
                tags = nodo.entidades[ix].tags
                return [t.to_dict() for t in tags], 200
            return dict(success=False, errors=f"No existe entidad [{entidad}] en nodo [{nombre}]"), 404
        except Exception as e:
            return default_error_handler(e)

    @api.expect(ser_from.tags)
    def delete(self, nombre, entidad):
        """ Elimina una lista de tags de una entidad en un nodo """
        try:
            tags = dict(request.json)["tags"]
            nodo = SRNode.objects(nombre=nombre).first()
            if nodo is None:
                return dict(success=False, errors=f"No existe nodo [{nombre}]"), 404
            success, msg = nodo.remove_tags_in_entity(tag_list=tags, nombre_entidad=entidad)
            if success:
                nodo.save()
                correct, r = nodo.search_entity(entidad)
                return (r.to_dict(), 200) if correct else (r, 400)
            return dict(success=False, errors=msg), 400
        except Exception as e:
            return default_error_handler(e)


@ns.route('/nodo/<string:nombre>/<string:entidad>/tag')
class SRTagAPI(Resource):
    @api.expect(ser_from.tagname)
    def put(self, nombre, entidad):
        """ Modifica la configuración de una tag
            Si la entidad no existe en el nodo entonces habrá un error 404
            Si la tag no existe en la entidad entonces se crea una nueva
        """
        try:
            nodo = SRNode.objects(nombre=nombre).first()
            if nodo is None:
                return dict(success=False, errors=f"No existe nodo [{nombre}]"), 404
            # from body content convert to SRTag object
            tag = SRTag(**request.json)
            success, msg = nodo.add_or_replace_tags_in_entity([tag], entidad)
            if success:
                nodo.save()
                correct, r = nodo.search_entity(entidad)
                return (r.to_dict(), 200) if correct else (r, 400)
            return dict(success=False, errors=msg), 400
        except Exception as e:
            return default_error_handler(e)

    @api.expect(ser_from.name_delete)
    def delete(self, nombre, entidad):
        """ Elimina la configuración de una tag
            Si la entidad no existe en el nodo entonces habrá un error 404
            Si la tag no existe en la entidad entonces habrá un error 404
        """
        try:
            nodo = SRNode.objects(nombre=nombre).first()
            if nodo is None:
                return dict(success=False, errors=f"No existe nodo [{nombre}]"), 404
            # nombre a eliminar
            name_delete = dict(request.json)["eliminar_elemento"]
            # check if entity exists:
            check_entity = [i for i, e in enumerate(nodo.entidades) if entidad == e.nombre]
            if len(check_entity) == 0:
                return dict(success=False, errors=f"No existe la entidad [{entidad}] en nodo [{nombre}]")
            id_e = check_entity[0]
            # if entity exists then use tags from that entity (id_e)
            new_tags = list()
            for t in nodo.entidades[id_e].tags:
                if t.tag_name != name_delete:
                    new_tags.append(t)

            nodo.entidades[id_e].tags = new_tags
            nodo.save()
            return nodo.entidades[id_e].to_dict(), 200
        except Exception as e:
            return default_error_handler(e)


@ns.route('/nodos/')
@ns.route('/nodos/<string:filter>')
class SRNodoAPI(Resource):
    def get(self, filter=None):
        """
        Muestra todos los nombres de los nodos existentes si el filtro está vacio
        Los caracteres * son comodines de busqueda
        Ejemplo: ['pala', 'alambre', 'pétalo'] ,
                *ala* => 'pala', 'alambre'
                ala* => 'alambre'
        """
        try:
            nodes = SRNode.objects
            if len(nodes) == 0:
                return dict(success=False, errors=f"No hay nodos en la base de datos"), 404
            if filter is None or len(filter) == 0:
                to_show = [n.to_summary() for n in nodes]
                return to_show, 200
            filter = str(filter).replace("*", ".*")
            regex = re.compile(filter, re.IGNORECASE)
            nodes = SRNode.objects(nombre=regex)
            to_show = [n.to_summary() for n in nodes]
            return to_show, 200
        except Exception as e:
            return default_error_handler(e)


@ns.route('/nodo/<string:tipo>/<string:nombre>/from-excel')
class SRNodeFromExcel(Resource):
    @api.response(200, 'El nodo ha sido ingresado de manera correcta')
    @api.expect(parsers.excel_upload)
    def post(self, nombre, tipo):
        """ Permite añadir un nodo mediante un archivo excel
            Si el nodo ha sido ingresado correctamente, entonces el código es 200
            Si el nodo ya existe entonces error 409
        """
        try:
            args = parsers.excel_upload.parse_args()
            nodo = SRNode.objects(nombre=nombre).first()
            if nodo is not None:
                return dict(success=False, errors=f"El nodo {[nombre]} ya existe"), 409

            if args['excel_file'].mimetype in 'application/xls, application/vnd.ms-excel,  application/xlsx' \
                                              'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
                excel_file = args['excel_file']
                filename = excel_file.filename
                stream_excel_file = excel_file.stream.read()
                # path del archivo temporal a guardar para poderlo leer inmediatamente
                temp_file = os.path.join(init.TEMP_PATH, f"{str(randint(0, 100))}_{filename}")
                with open(temp_file, 'wb') as f:
                    f.write(stream_excel_file)

                df_main = pd.read_excel(temp_file, sheet_name="main")
                df_tags = pd.read_excel(temp_file, sheet_name="tags")

                # una vez leído, eliminar archivo temporal
                os.remove(temp_file)

                # create a virtual node:
                v_node = SRNodeFromDataFrames(nombre, tipo, df_main, df_tags)
                success, msg = v_node.validate()
                if not success:
                    return dict(success=False, errors=msg), 400
                # create a final node to save if is successful
                success, node = v_node.create_node()
                if not success:
                    return dict(success=False, errors=str(node)), 400
                node.actualizado = dt.datetime.now()
                node.save()
                # Guardar como archivo Excel con versionamiento
                destination = os.path.join(init.SREMOTO_REPO, filename)
                save_excel_file_from_bytes(destination=destination, stream_excel_file=stream_excel_file)
                return node.to_summary(), 200
            else:
                return dict(success=False, errors="El formato del archivo no es aceptado"), 400
        except Exception as e:
            return default_error_handler(e)

    @api.response(200, 'El nodo ha sido actualizado de manera correcta')
    @api.expect(parsers.excel_upload_w_option)
    def put(self, nombre, tipo):
        """ Permite actualizar un nodo mediante un archivo excel
            Si el nodo no existe entonces error 404
            DEFAULT:
            Si las entidades internas no existen entonces se añaden a la lista de entidades
            Las tags se actualizan conforme a lo especificado en el archivo
            REEMPLAZAR:
            El nodo completo es sustituido de acuerdo a lo especificado en el archivo
        """

        args = parsers.excel_upload.parse_args()
        nodo = SRNode.objects(nombre=nombre).first()
        if nodo is None:
            return dict(success=False, errors=f"El nodo {[nombre]} no existe"), 400

        try:
            if args['excel_file'].mimetype in 'application/xls, application/vnd.ms-excel,  application/xlsx' \
                                              'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
                excel_file = args['excel_file']
                filename = excel_file.filename
                stream_excel_file = excel_file.stream.read()

                # path del archivo temporal a guardar para poderlo leer inmediatamente
                temp_file = os.path.join(init.TEMP_PATH, f"{str(randint(0, 100))}_{filename}")
                with open(temp_file, 'wb') as f:
                    f.write(stream_excel_file)

                df_main = pd.read_excel(temp_file, sheet_name="main")
                df_tags = pd.read_excel(temp_file, sheet_name="tags")

                # una vez leído, eliminar archivo temporal
                os.remove(temp_file)
                # create a virtual node:
                v_node = SRNodeFromDataFrames(nombre, tipo, df_main, df_tags)
                success, msg = v_node.validate()
                if not success:
                    return dict(success=False, errors=msg), 400
                # create a final node to save if is successful
                success, new_node = v_node.create_node()
                if not success:
                    return dict(success=False, errors=str(new_node)), 400
                nodo.actualizado = dt.datetime.now()
                if args['option'] is None:
                    success, msg = nodo.add_or_replace_entities(new_node.entidades)
                    if not success:
                        return dict(success=False, errors=str(msg)), 400
                    nodo.save()
                elif str(args['option']).upper() == "REEMPLAZAR":
                    nodo.delete()
                    new_node.save()
                    nodo = new_node
                # Guardar como archivo Excel con versionamiento
                destination = os.path.join(init.SREMOTO_REPO, filename)
                save_excel_file_from_bytes(destination=destination, stream_excel_file=stream_excel_file)
                return nodo.to_summary(), 200
            else:
                return dict(success=False, errors="El formato del archivo no es aceptado"), 400
        except Exception as e:
            return default_error_handler(e)


def save_excel_file_from_bytes(destination, stream_excel_file):
    try:
        n = 7
        last_file = destination.replace(".xls", f"@{n}.xls")
        first_file = destination.replace(".xls", "@1.xls")
        for i in range(n, 0, -1):
            file_n = destination.replace(f".xls", f"@{str(i)}.xls")
            file_n_1 = destination.replace(f".xls", f"@{str(i + 1)}.xls")
            if os.path.exists(file_n):
                os.rename(file_n, file_n_1)
        if os.path.exists(last_file):
            os.remove(last_file)
        if not os.path.exists(first_file) and os.path.exists(destination):
            os.rename(destination, first_file)

    except Exception as e:
        version = dt.datetime.now().strftime("@%Y-%b-%d_%Hh%M")
        destination = destination.replace(".xls", f"{version}.xls")

    with open(destination, 'wb') as f:
        f.write(stream_excel_file)


"""
@ns.route('/upload')
class UploadFile(Resource):
    @api.response(200, 'Archivo subido correctamente')
    @api.expect(parsers.file_upload)
    def post(self):
        try:
            args = parsers.file_upload.parse_args()
            print(args['file'].mimetype)
            return dict(success=True), 200
        except Exception as e:
            return default_error_handler(e)
"""
