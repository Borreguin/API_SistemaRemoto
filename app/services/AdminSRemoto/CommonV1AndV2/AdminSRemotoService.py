from __future__ import annotations

import math
import re
from typing import Dict

from starlette import status

from app.common.util import to_dict
from app.db.constants import V1_SR_NODE_LABEL, V2_SR_NODE_LABEL
from app.db.db_util import node_query, find_node_by_name_and_type, update_summary_node_info, create_node
from app.schemas.RequestSchemas import BasicNodeInfoRequest
from app.utils.excel_util import *


def put_activa_desactiva_nodo(id_nodo: str = "ID del nodo a cambiar", activado=True, version:str=None) -> Tuple[dict, int]:
    nodo = node_query(id_nodo, version)
    if nodo is None:
        return dict(success=False, nodo=None, msg="No encontrado"), status.HTTP_404_NOT_FOUND
    nodo.actualizado, nodo.activado = dt.datetime.now(), activado
    msg_active = "Nodo activado" if activado else "Nodo desactivado"
    success, msg = nodo.save_safely()
    return (dict(success=success, nodo=nodo.to_dict(), msg=msg_active if success else "No able to activate"),
            status.HTTP_200_OK if success else status.HTTP_400_BAD_REQUEST)


def delete_elimina_nodo_usando_ID_como_referencia(id: str, version=None) -> Tuple[dict, int]:
    node = node_query(id, version)
    if node is None:
        return dict(success=False, nodo=None, msg="No se encontró el nodo"), status.HTTP_404_NOT_FOUND
    if version == V1_SR_NODE_LABEL:
        node.delete()
    else:
        node.delete_deeply()
    return dict(success=True, nodo=node.to_dict(), msg="Nodo eliminado"), status.HTTP_200_OK


def put_actualiza_cambios_menores_en_nodo(id: str, request_data: BasicNodeInfoRequest, version=None) -> Tuple[dict, int]:
    node = node_query(id, version)
    is_new = id == 'null'
    if node is None and is_new:
        node = create_node(request_data.tipo, request_data.nombre, request_data.activado, version)
    if node is None:
        return dict(success=False, nodo=None, msg=f"No se encontró el nodo {id}"), status.HTTP_404_NOT_FOUND
    success, msg, node = update_summary_node_info(node, to_dict(request_data), replace=not is_new)
    if not success:
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    success, msg = node.save_safely()
    return dict(success=success, nodo=node.to_summary(),
                msg=msg), status.HTTP_200_OK if success else status.HTTP_400_BAD_REQUEST


def post_crea_nuevo_nodo_usando_ID(id, request_data: BasicNodeInfoRequest, version=None) -> Tuple[dict, int]:
    node = node_query(id, version)
    if node is not None:
        return dict(success=False, msg="El nodo ya existe, no puede ser creado"), status.HTTP_400_BAD_REQUEST

    new_nodo = create_node(request_data.tipo, request_data.nombre, request_data.activado, version)
    success, msg, new_nodo = update_summary_node_info(new_nodo, to_dict(request_data))
    if not success:
        return dict(success=False, nodo=None, msg=msg), status.HTTP_400_BAD_REQUEST
    success, msg = new_nodo.save_safely()
    return dict(success=success, nodo=new_nodo.to_summary(), msg=msg), status.HTTP_200_OK if success else status.HTTP_400_BAD_REQUEST


def get_retorna_entidades_de_nodo(tipo: str, nombre: str, entidad_tipo: str, entidad_nombre: str, version=None) -> Tuple[dict | None, int]:
    nodo = find_node_by_name_and_type(nombre=nombre, tipo=tipo, version=version)
    if nodo is None:
        return None, status.HTTP_404_NOT_FOUND
    for entidad in nodo.entidades:
        if entidad.entidad_tipo == entidad_tipo and entidad.entidad_nombre == entidad_nombre:
            return entidad.to_dict(), status.HTTP_200_OK
    return None, status.HTTP_404_NOT_FOUND


def get_muestra_todos_los_nombres_nodos_existentes(filter_str=None, version=None) -> Tuple[dict, int]:
    if version is None:
        version = V1_SR_NODE_LABEL
    nodes = SRNode.objects(document=version).as_pymongo()
    if nodes.count() == 0:
        return dict(success=False, msg=f"No hay nodos en la base de datos"), status.HTTP_404_NOT_FOUND
    if filter_str is None or len(filter_str) == 0:
        _nodes = get_details_from_dict(nodes, version)
        return dict(success=True, nodos=_nodes, msg=f"Se han obtenido {len(_nodes)} nodos"), status.HTTP_200_OK
    filter_str = str(filter_str).replace("*", ".*")
    regex = re.compile(filter_str, re.IGNORECASE)
    nodes = SRNode.objects(nombre=regex)
    to_show = [n.to_summary() for n in nodes]
    return dict(success=True, nodos=to_show, msg=f"Se han obtenido {len(to_show)} nodos"), status.HTTP_200_OK

def get_details_from_dict(nodes: dict, version:str=None) -> List[Dict]:
    # creando un resumen rápido de los nodos:
    _nodes = list()
    for ix, node in enumerate(nodes):
        if node["document"] != version:
            continue
        n_installations, n_tags, entidades, node["_id"] = 0, 0, list(), str(node["_id"])
        if "entidades" not in node.keys():
            node["entidades"] = list()
            _nodes.append(node)
            continue

        for entidad in node["entidades"]:
            new_entity = dict()
            if version == V1_SR_NODE_LABEL:
                new_entity, n_tags = get_rtu_details(entidad, n_tags)
            if version == V2_SR_NODE_LABEL:
                new_entity, n_installations = get_installations_details(entidad, n_installations)
            entidades.append(new_entity)
        # creando el resumen del nodo
        node["actualizado"] = str(node["actualizado"])
        node["entidades"] = entidades
        node["document_id"] = node["_id"]
        _nodes.append(node)
    return _nodes

def get_rtu_details(entidad:dict, n_tags) -> Tuple[dict, int]:
    if "utrs" in entidad.keys():
        entidad["utrs"] = sorted(entidad["utrs"], key=lambda i: i['utr_nombre'])
        n_rtu = len(entidad["utrs"])
        n_tag_inside = sum([len(rtu["tags"]) for rtu in entidad["utrs"]])
        n_tags += n_tag_inside
        for utr in entidad["utrs"]:
            utr.pop("consignaciones", None)
            utr.pop("tags", None)
            utr['longitude'] = utr['longitude'] \
                if 'longitude' in utr.keys() and not math.isnan(utr['longitude']) else 0
            utr['latitude'] = utr['latitude'] \
                if 'latitude' in utr.keys() and not math.isnan(utr['latitude']) else 0
    else:
        n_rtu = 0
        n_tag_inside = 0
    entidad["n_utrs"] = n_rtu
    entidad["n_tags"] = n_tag_inside
    return entidad, n_tags

def get_installations_details(entidad, n_installations):
    inner_installations = 0
    if "instalaciones" in entidad.keys():
        inner_installations = len(entidad["instalaciones"])
        entidad.pop("instalaciones", None)

    entidad["n_installations"] = inner_installations
    entidad["created"] = str(entidad["created"])
    return entidad, n_installations + inner_installations