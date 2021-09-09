# Created by Roberto Sanchez at 4/16/2019
# -*- coding: utf-8 -*-
""""
    Servicio Web de Sistema Remoto:
        - Permite la administración de Nodos, Entidades y Tags
        - Serializar e ingresar datos

    If you need more information. Please contact the email above: rg.sanchez.a@gmail.com
    "My work is well done to honor God at any time" R Sanchez A.
    Mateo 6:33
"""

from flask_restplus import Resource
from flask import request, send_from_directory
import re
# importando configuraciones iniciales
from flask_app.my_lib.utils import set_max_age_to_response
from flask_app.api.services.restplus_config import api
from flask_app.api.services.sRemoto import serializers as srl
from flask_app.api.services.sRemoto import parsers
# importando clases para leer desde MongoDB
from flask_app.dto.mongo_engine_handler.sRNode import *
from random import randint

# configurando logger y el servicio web
ns = api.namespace('admin-sRemoto', description='Relativas a la administración de nodos de Sistema Remoto')

ser_from = srl.sRemotoSerializers(api)
api = ser_from.add_serializers()


@ns.route('/nodo/<string:tipo>/<string:nombre>')
class SRNodeAPI(Resource):

    def get(self, tipo: str = "Tipo de nodo", nombre: str = "Nombre del nodo a buscar"):
        """ Busca si un nodo tipo SRNode existe en base de datos """
        nodo = SRNode.objects(nombre=nombre, tipo=tipo).first()
        if nodo is None:
            return nodo, 404
        return nodo.to_dict(), 200

    @api.expect(ser_from.name_update)
    def put(self, tipo: str = "Tipo de nodo", nombre: str = "Nombre del nodo a cambiar"):
        """
        Actualiza el nombre de un nodo
        """
        new_name = request.json["nuevo_nombre"]
        nodo = SRNode.objects(nombre=nombre, tipo=tipo).first()
        if nodo is None:
            return nodo, 404
        nodo.nombre = new_name
        nodo.actualizado = dt.datetime.now()
        nodo.save()
        return nodo.to_dict(), 200


@ns.route('/nodo/id/<string:id_nodo>/activado')
class SRNodeAPIActivated(Resource):

    def put(self, id_nodo: str = "ID del nodo a cambiar"):
        """
        Activa el nodo
        """
        nodo = SRNode.objects(id_node=id_nodo).first()
        if nodo is None:
            return dict(success=False, nodo=None, msg="No encontrado"), 404
        nodo.actualizado = dt.datetime.now()
        nodo.activado = True
        nodo.save()
        return dict(success=True, nodo=nodo.to_dict(), msg="Nodo activado"), 200


@ns.route('/nodo/id/<string:id_nodo>/desactivado')
class SRNodeAPIDeactivated(Resource):

    def put(self, id_nodo: str = "ID del nodo a cambiar"):
        """
        Desactiva el nodo
        """
        nodo = SRNode.objects(id_node=id_nodo).first()
        if nodo is None:
            return dict(success=False, nodo=None, msg="No encontrado"), 404
        nodo.actualizado = dt.datetime.now()
        nodo.activado = False
        nodo.save()
        return dict(success=True, nodo=nodo.to_dict(), msg="Nodo desactivado"), 200


@ns.route('/nodo/<string:tipo>/<string:nombre>/<string:entidad_tipo>/<string:entidad_nombre>')
class SREntidadAPI(Resource):
    def get(self, tipo: str = "Tipo nodo", nombre: str = "Nombre nodo", entidad_tipo: str = "Entidad tipo",
            entidad_nombre: str = "Entidad nombre"):
        """ Retorna las entidades de un nodo """
        nodo = SRNode.objects(nombre=nombre, tipo=tipo).first()
        if nodo is None:
            return nodo, 404
        for entidad in nodo.entidades:
            if entidad.entidad_tipo == entidad_tipo and entidad.entidad_nombre == entidad_nombre:
                return entidad.to_dict(), 200
        return None, 404


@ns.route('/rtu/<string:id_nodo>/<string:id_entidad>')
class SRRTUSAPI(Resource):

    def get(self, id_nodo: str = "id nodo", id_entidad: str = "id entidad"):
        """ Regresa la lista de RTU de una entidad
            Id nodo: id único del nodo
            Id entidad: id único de la entidad
            <b>404</b> Si el nodo o la entidad no existe
        """
        nodo = SRNode.objects(id_node=id_nodo).as_pymongo().first()
        if nodo is None:
            return dict(success=False, msg="No se encuentra el nodo"), 404
        idx = None
        for ix, _entidad in enumerate(nodo["entidades"]):
            if _entidad["id_entidad"] == id_entidad:
                idx = ix
                break
        if idx is None:
            return dict(success=False, msg="No se encuentra la entidad"), 404

        utrs_list = nodo["entidades"][idx]["utrs"]
        for utr in utrs_list:
            utr.pop('consignaciones')
            utr.pop("tags")
            utr['longitude'] = utr['longitude'] \
                if 'longitude' in utr.keys() and not math.isnan(utr['longitude']) else 0
            utr['latitude'] = utr['latitude'] \
                if 'latitude' in utr.keys() and not math.isnan(utr['latitude']) else 0
        utrs_list = sorted(utrs_list, key=lambda i: i['utr_nombre'])
        return dict(success=True, utrs=utrs_list), 200

    @api.expect(ser_from.rtu)
    def post(self, id_nodo: str = "id nodo", id_entidad: str = "id entidad"):
        """ Ingresa una nueva RTU en una entidad si esta no existe, caso contrario la edita
            Id nodo: id único del nodo
            Id entidad: id único de la entidad
            <b>404</b> Si el nodo o la entidad no existe
        """
        request_data = dict(request.json)
        nodo = SRNode.objects(id_node=id_nodo).first()
        if nodo is None:
            return dict(success=False, msg="No se encuentra el nodo"), 404
        idx = None
        for ix, _entidad in enumerate(nodo.entidades):
            if _entidad.id_entidad == id_entidad:
                idx = ix
                break
        if idx is None:
            return dict(success=False, msg="No se encuentra la entidad"), 404
        rtu = SRUTR(**request_data)
        success, msg = nodo.entidades[idx].add_or_rename_utrs([rtu])
        utrs = nodo.entidades[idx].utrs
        if success:
            rtu.create_consignments_container()
            nodo.save()
            return dict(success=success, utrs=[u.to_dict() for u in utrs],  msg=msg), 200
        else:
            return dict(success=success, utrs=[u.to_dict() for u in utrs], msg=msg), 409

    @api.expect(ser_from.rtu_id)
    def delete(self, id_nodo: str = "id nodo", id_entidad: str = "id entidad"):
        """ Elimina una RTU en una entidad
            Id nodo: id único del nodo
            Id entidad: id único de la entidad
            <b>404</b> Si el nodo o la entidad no existe
        """
        request_data = dict(request.json)
        nodo = SRNode.objects(id_node=id_nodo).first()
        if nodo is None:
            return dict(success=False, msg="No se encuentra el nodo"), 404
        idx = None
        for ix, _entidad in enumerate(nodo.entidades):
            if _entidad.id_entidad == id_entidad:
                idx = ix
                break
        if idx is None:
            return dict(success=False, msg="No se encuentra la entidad"), 404
        success, msg = nodo.entidades[idx].remove_utrs([request_data["id_utr"]])
        if success:
            nodo.save()
            return dict(success=success, msg=msg, utrs=[r.to_dict() for r in nodo.entidades[idx].utrs]), 200
        else:
            return dict(success=success, msg=msg), 409


@ns.route('/rtu/<string:id_nodo>/<string:id_entidad>/<string:id_utr>')
class SRRTUAPI(Resource):

    @staticmethod
    def get(id_nodo: str = "id nodo", id_entidad: str = "id entidad", id_utr: str = "id UTR"):
        """ Regresa la cofiguración de la RTU
            Id nodo: id único del nodo
            Id entidad: id único de la entidad
            Id utr: id único de la entidad
            <b>404</b> Si el nodo, la entidad o UTR no existe
        """
        nodo = SRNode.objects(id_node=id_nodo).first()
        if nodo is None:
            return dict(success=False, msg="No se encuentra el nodo"), 404
        idx = None
        for ix, _entidad in enumerate(nodo.entidades):
            if _entidad.id_entidad == id_entidad:
                idx = ix
                break
        if idx is None:
            return dict(success=False, msg="No se encuentra la entidad"), 404

        for ix, _utr in enumerate(nodo.entidades[idx].utrs):
            if _utr.id_utr == id_utr or _utr.utr_code == id_utr:
                return dict(success=True, msg="RTU encontrada", utr=_utr.to_dict()), 200

        return dict(success=False, msg="RTU no encontrada"), 404


@ns.route('/tags/<string:id_nodo>/<string:id_entidad>/<string:id_utr>')
class SRTAGSAPI(Resource):

    @staticmethod
    def get(id_nodo: str = "id nodo", id_entidad: str = "id entidad", id_utr: str = "id utr"):
        """ Regresa la lista de TAGS de una UTR
            Id nodo: id único del nodo
            Id entidad: id único de la entidad
            Id utr: id único de la UTR
            <b>404</b> Si el nodo, entidad. no existe
        """
        nodo = SRNode.objects(id_node=id_nodo).first()
        if nodo is None:
            return dict(success=False, msg="No se encuentra el nodo"), 404
        idx = None
        for ix, _entidad in enumerate(nodo.entidades):
            if _entidad.id_entidad == id_entidad:
                idx = ix
                break
        if idx is None:
            return dict(success=False, msg="No se encuentra la entidad"), 404

        for ix, _utr in enumerate(nodo.entidades[idx].utrs):
            if _utr.id_utr == id_utr or _utr.utr_code == id_utr:
                return dict(success=True, tags=[t.to_dict() for t in _utr.tags], msg="Tags encontradas"), 200
        return dict(success=False, msg="No se encuentra la UTR"), 404

    @api.expect(ser_from.list_tagname)
    def post(self, id_nodo: str = "id nodo", id_entidad: str = "id entidad", id_utr: str = "id utr"):
        """ Ingresa una lista de TAGS en una UTR si estas no existen, caso contrario las edita
            Id nodo: id único del nodo
            Id entidad: id único de la entidad
            Id UTR: id o código único de la UTR
            <b>404</b> Si el nodo o la entidad no existe
        """
        request_data = dict(request.json)
        nodo = SRNode.objects(id_node=id_nodo).first()
        if nodo is None:
            return dict(success=False, msg="No se encuentra el nodo"), 404
        idx = None
        for ix, _entidad in enumerate(nodo.entidades):
            if _entidad.id_entidad == id_entidad:
                idx = ix
                break
        if idx is None:
            return dict(success=False, msg="No se encuentra la entidad"), 404

        for ix, _utr in enumerate(nodo.entidades[idx].utrs):
            if _utr.id_utr == id_utr or _utr.utr_code == id_utr:
                SRTags, n_valid = list(), 0
                for tag in request_data["tags"]:
                    if len(str(tag["tag_name"]).strip()) <= 4 or len(str(tag["filter_expression"]).strip()) == 0:
                        continue
                    SRTags.append(SRTag(tag_name=str(tag["tag_name"]).strip(),
                                        filter_expression=str(tag["filter_expression"]).strip(),
                                        activado=tag["activado"]))
                    n_valid += 1
                n_tags = len(nodo.entidades[idx].utrs[ix].tags)
                success, msg = nodo.entidades[idx].utrs[ix].add_or_replace_tags(SRTags)
                if success and n_valid > 0:
                    nodo.save()
                    tags = [t.to_dict() for t in nodo.entidades[idx].utrs[ix].tags]
                    n_inserted = len(tags) - n_tags
                    n_edited = n_valid - n_inserted
                    msg = f"TAGS: -insertadas {n_inserted} -editadas {n_edited} "
                    return dict(success=success, msg=msg, tags=tags), 200
                if n_valid == 0:
                    return dict(success=False, msg="No hay Tags válidas a ingresar"), 409
                return dict(success=success, msg=msg), 409
        return dict(success=False, msg="No se encuentra la UTR"), 404

    @api.expect(ser_from.list_edited_tagname)
    def put(self, id_nodo: str = "id nodo", id_entidad: str = "id entidad", id_utr: str = "id utr"):
        """ Edita una lista de TAGS en una UTR basado en tag_name_original
            Id nodo: id único del nodo
            Id entidad: id único de la entidad
            Id UTR: id o código único de la UTR
            <b>404</b> Si el nodo, entidad o UTR no existe
        """
        request_data = dict(request.json)
        nodo = SRNode.objects(id_node=id_nodo).first()
        if nodo is None:
            return dict(success=False, msg="No se encuentra el nodo"), 404
        idx = None
        for ix, _entidad in enumerate(nodo.entidades):
            if _entidad.id_entidad == id_entidad:
                idx = ix
                break
        if idx is None:
            return dict(success=False, msg="No se encuentra la entidad"), 404

        for ix, _utr in enumerate(nodo.entidades[idx].utrs):
            if _utr.id_utr == id_utr or _utr.utr_code == id_utr:
                tags_req = request_data["tags"]
                tag_names = [t.pop("tag_name_original", None) for t in tags_req]
                [t.pop("edited", None) for t in tags_req]
                SRTags = [SRTag(**d) for d in tags_req]
                success, (n_remove, msg) = nodo.entidades[idx].utrs[ix].remove_tags(tag_names)
                if not success:
                    return dict(success=success, msg=msg)
                success, msg = nodo.entidades[idx].utrs[ix].add_or_replace_tags(SRTags)
                if success:
                    nodo.save()
                    tags = [t.to_dict() for t in nodo.entidades[idx].utrs[ix].tags]
                    return dict(success=success, msg=f"{nodo.entidades[idx].utrs[ix]} TAGS: -editadas: {n_remove} "
                                                     f"-añadidas: {len(tags_req) - n_remove}", tags=tags), 200
                return dict(success=success, msg=f"{nodo.entidades[idx].utrs[ix]} {msg}"), 409
        return dict(success=False, msg="No se encuentra la UTR"), 404

    @api.expect(ser_from.tags)
    def delete(self, id_nodo: str = "id nodo", id_entidad: str = "id entidad", id_utr: str = "id utr"):
        """ Elimina una lista de tags basado en las ids de Nodo, Entidad, UTR
            Id nodo: id único del nodo
            Id entidad: id único de la entidad
            Id UTR: id o código único de la UTR
            <b>404</b> Si el nodo, entidad o UTR no existe
        """

        request_data = dict(request.json)
        nodo = SRNode.objects(id_node=id_nodo).first()
        if nodo is None:
            return dict(success=False, msg="No se encuentra el nodo"), 404
        idx = None
        for ix, _entidad in enumerate(nodo.entidades):
            if _entidad.id_entidad == id_entidad:
                idx = ix
                break
        if idx is None:
            return dict(success=False, msg="No se encuentra la entidad"), 404

        for ix, _utr in enumerate(nodo.entidades[idx].utrs):
            if _utr.id_utr == id_utr or _utr.utr_code == id_utr:
                tag_names = request_data["tags"]
                success, (n_remove, msg) = nodo.entidades[idx].utrs[ix].remove_tags(tag_names)
                if not success:
                    return dict(success=success, msg=msg), 409
                nodo.save()
                tags = [t.to_dict() for t in nodo.entidades[idx].utrs[ix].tags]
                return dict(success=success, msg=f"TAGS: -eliminadas: {n_remove} "
                                                 f"-no encontradas: {len(tag_names) - n_remove}", tags=tags), 200

        return dict(success=False, msg="No se encuentra la UTR"), 404


@ns.route('/nodo/id/<string:id>')
class SRNodeIDAPI(Resource):
    def delete(self, id):
        """ Elimina nodo usando su ID como referencia """
        node = SRNode.objects(id_node=id).first()
        if node is None:
            return dict(success=False, nodo=None, msg="No se encontró el nodo"), 404
        node.delete()
        return dict(success=True, nodo=node.to_dict(), msg="Nodo eliminado"), 200

    def put(self, id):
        """ Actualiza cambios menores en el nodo (nombres, tipos, activaciones) """
        request_data = dict(request.json)
        id_node = request_data["id_node"]
        node = SRNode.objects(id_node=id_node).first()
        if node is None:
            return dict(success=False, nodo=None, msg="No se encontró el nodo"), 404
        success, msg = node.update_summary_info(request_data)
        if not success:
            return dict(success=False, msg=msg), 400
        node.save()
        return dict(success=True, nodo=node.to_summary(), msg="Se han guardado los cambios"), 200

    def post(self, id):
        """ Crea un nuevo nodo usando ID """
        request_data = dict(request.json)
        nodo = SRNode.objects(id_node=id).first()
        if nodo is not None:
            return dict(success=False, msg="El nodo ya existe, no puede ser creado"), 400
        nodo = SRNode(nombre=request_data["nombre"], tipo=request_data["tipo"], activado=request_data["activado"])
        success, msg = nodo.update_summary_info(request_data)
        if not success:
            return dict(success=False, nodo=None, msg=msg), 400
        try:
            nodo.save()
        except Exception as e:
            # problema al existir entidad nula en un nodo ya existente
            if "entidades.id_entidad_1 dup key" in str(e):
                entidad = SREntity(entidad_nombre="Nombre " + str(randint(0, 1000)), entidad_tipo="Empresa")
                nodo.add_or_replace_entities([entidad])
                nodo.save()
        return dict(success=True, nodo=nodo.to_summary(), msg="Nodo creado"), 200

@ns.route('/nodos/')
@ns.route('/nodos/<string:filter>')
class SRNodoAPI(Resource):
    def get(self, filter_str=None):
        """
        Muestra todos los nombres de los nodos existentes si el filtro está vacio
        Los caracteres * son comodines de busqueda
        Ejemplo: ['pala', 'alambre', 'pétalo'] ,
                *ala* => 'pala', 'alambre'
                ala* => 'alambre'
        """
        nodes = SRNode.objects().as_pymongo().exclude('id')
        if nodes.count() == 0:
            return dict(success=False, msg=f"No hay nodos en la base de datos"), 404
        if filter_str is None or len(filter_str) == 0:
            # creando un resumen rápido de los nodos:
            _nodes = list()
            for ix, node in enumerate(nodes):
                n_tags = 0
                entidades = list()
                if "entidades" not in node.keys():
                    continue
                for entidad in node["entidades"]:
                    if "utrs" in entidad.keys():
                        entidad["utrs"] = sorted(entidad["utrs"], key=lambda i: i['utr_nombre'])
                        n_rtu = len(entidad["utrs"])
                        n_tag_inside = sum([len(rtu["tags"]) for rtu in entidad["utrs"]])
                        n_tags += n_tag_inside
                        for utr in entidad["utrs"]:
                            utr.pop("consignaciones")
                            utr.pop("tags")
                            utr['longitude'] = utr['longitude'] \
                                if 'longitude' in utr.keys() and not math.isnan(utr['longitude']) else 0
                            utr['latitude'] = utr['latitude'] \
                                if 'latitude' in utr.keys() and not math.isnan(utr['latitude']) else 0
                    else:
                        n_rtu = 0
                        n_tag_inside = 0
                    entidad["n_utrs"] = n_rtu
                    entidad["n_tags"] = n_tag_inside
                    entidades.append(entidad)
                # creando el resumen del nodo
                node["actualizado"] = str(node["actualizado"])
                node["entidades"] = entidades
                _nodes.append(node)
            # to_show = [n.to_summary() for n in nodes]
            return dict(success=True, nodos=_nodes, msg=f"Se han obtenido {len(_nodes)} nodos"), 200
        filter_str = str(filter_str).replace("*", ".*")
        regex = re.compile(filter_str, re.IGNORECASE)
        nodes = SRNode.objects(nombre=regex)
        to_show = [n.to_summary() for n in nodes]
        return dict(success=True, nodos=to_show, msg=f"Se han obtenido {len(to_show)} nodos"), 200


# se puede consultar este servicio como: /url?nid=<cualquier_valor_random>
@ns.route('/nodo/<string:tipo>/<string:nombre>/from-excel')
class SRNodeFromExcel(Resource):
    @api.response(200, 'El nodo ha sido ingresado de manera correcta')
    @api.expect(parsers.excel_upload)
    def post(self, tipo, nombre):
        """ Permite añadir un nodo mediante un archivo excel
            Si el nodo ha sido ingresado correctamente, entonces el código es 200
            Si el nodo ya existe entonces error 409
            r simplemente permite la actualización de la petición, puede ser cualquier valor
        """
        args = parsers.excel_upload.parse_args()
        nodo = SRNode.objects(nombre=nombre).first()
        if nodo is not None:
            return dict(success=False, msg=f"El nodo {[nombre]} ya existe"), 409

        if args['excel_file'].mimetype in 'application/xls, application/vnd.ms-excel,  application/xlsx' \
                                          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
            excel_file = args['excel_file']
            filename = excel_file.filename
            stream_excel_file = excel_file.stream.read()
            # path del archivo temporal a guardar para poderlo leer inmediatamente
            temp_file = os.path.join(init.TEMP_REPO, f"{str(randint(0, 100))}_{filename}")
            with open(temp_file, 'wb') as f:
                f.write(stream_excel_file)

            df_main = pd.read_excel(temp_file, sheet_name="main")
            df_tags = pd.read_excel(temp_file, sheet_name="tags")

            # una vez leído, eliminar archivo temporal
            os.remove(temp_file)

            # create a virtual node:
            v_node = SRNodeFromDataFrames(tipo, nombre, df_main, df_tags)
            success, msg = v_node.validate()
            if not success:
                # el archivo no contiene el formato correcto
                return dict(success=False, msg=msg), 400
            # create a final node to save if is successful
            success, node = v_node.create_node()
            if not success:
                return dict(success=False, msg=str(node)), 400
            node.actualizado = dt.datetime.now()
            node.save()
            for entity in node.entidades:
                for utr in entity.utrs:
                    utr.create_consignments_container()
            node.save()
            # Guardar como archivo Excel con versionamiento
            destination = os.path.join(init.SREMOTO_REPO, filename)
            save_excel_file_from_bytes(destination=destination, stream_excel_file=stream_excel_file)
            return dict(success=True, nodo=node.to_summary(), msg=msg), 200
        else:
            return dict(success=False, msg="El formato del archivo no es aceptado"), 400

    @api.response(200, 'El nodo ha sido actualizado de manera correcta')
    @api.expect(parsers.excel_upload_w_option)
    def put(self, tipo, nombre):
        """ Permite actualizar un nodo mediante un archivo excel
            Si el nodo no existe entonces error 404
            DEFAULT:
            Si las entidades internas no existen entonces se añaden a la lista de entidades
            Las tags se actualizan conforme a lo especificado en el archivo
            REEMPLAZAR:
            El nodo completo es sustituido de acuerdo a lo especificado en el archivo
        """

        args = parsers.excel_upload.parse_args()
        nodo = SRNode.objects(tipo=tipo, nombre=nombre).first()
        if nodo is None:
            return dict(success=False, msg=f"El nodo {[nombre]} no existe"), 400

        if args['excel_file'].mimetype in 'application/xls, application/vnd.ms-excel,  application/xlsx' \
                                          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
            excel_file = args['excel_file']
            filename = excel_file.filename
            stream_excel_file = excel_file.stream.read()

            # path del archivo temporal a guardar para poderlo leer inmediatamente
            temp_file = os.path.join(init.TEMP_REPO, f"{str(randint(0, 100))}_{filename}")
            with open(temp_file, 'wb') as f:
                f.write(stream_excel_file)

            df_main = pd.read_excel(temp_file, sheet_name="main", engine='openpyxl')
            df_tags = pd.read_excel(temp_file, sheet_name="tags", engine='openpyxl')

            # una vez leído, eliminar archivo temporal
            os.remove(temp_file)
            # create a virtual node:
            v_node = SRNodeFromDataFrames(tipo, nombre, df_main, df_tags)
            success, msg = v_node.validate()
            if not success:
                return dict(success=False, msg=msg), 400
            # create a final node to save if is successful
            success, new_node = v_node.create_node()
            if not success:
                return dict(success=False, msg=str(new_node)), 400
            nodo.actualizado = dt.datetime.now()
            if args['option'] is None:
                success, msg = nodo.add_or_replace_entities(new_node.entidades)
                if not success:
                    return dict(success=False, msg=str(msg)), 400
                nodo.save()
            elif str(args['option']).upper() == "REEMPLAZAR":
                nodo.delete()
                new_node.save()
                nodo = new_node
            # crear contenedores de consignaciones si fuera necesario
            for entity in nodo.entidades:
                for utr in entity.utrs:
                    utr.create_consignments_container()
            nodo.save()
            # Guardar como archivo Excel con versionamiento
            destination = os.path.join(init.SREMOTO_REPO, filename)
            save_excel_file_from_bytes(destination=destination, stream_excel_file=stream_excel_file)
            return dict(success=True, nodo=nodo.to_summary(), msg=msg), 200
        else:
            return dict(success=False, msg="El formato del archivo no es aceptado"), 400

    @api.response(200, 'El nodo ha sido descargado de manera correcta')
    def get(self, nombre, tipo):
        """ Descarga en formato excel la última versión del nodo
        """
        node = SRNode.objects(nombre=nombre, tipo=tipo).as_pymongo().exclude('id')
        if node.count() == 0:
            return dict(success=False, msg=f"No existe el nodo en la base de datos"), 404
        node_as_dict = node[0]
        success, df_main, df_tags, msg = SRDataFramesFromDict(node_as_dict).convert_to_DataFrames()
        if not success:
            return dict(success=False, msg=msg), 409
        file_name = f"{tipo}{nombre}.xlsx".replace("_", "@")
        path = os.path.join(init.TEMP_REPO, file_name)
        with pd.ExcelWriter(path) as writer:
            df_main.to_excel(writer, sheet_name="main")
            df_tags.to_excel(writer, sheet_name="tags")
        if os.path.exists(path):
            resp = send_from_directory(os.path.dirname(path), file_name, as_attachment=False)
            return set_max_age_to_response(resp, 5)


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
