from __future__ import annotations

import re

from bson import ObjectId
from mongoengine import Document, NotUniqueError

from app.db.constants import attributes_node, attr_id_entidad, attr_entidades, attr_entidad_tipo, attr_entidad_nombre, \
    attributes_entity, V2_SR_NODE_LABEL, V1_SR_NODE_LABEL
from app.db.v1.sRNode import SRNode, SREntity
from app.db.v2.entities.v2_sREntity import V2SREntity
from app.db.v2.entities.v2_sRInstallation import V2SRInstallation
from app.db.v2.entities.v2_sRNode import V2SRNode
from app.db.v2.util import find_collection_and_dup_key

regexNoUniqueEntity = 'entidades.id_entidad: [\\|\s]*"([a-z0-9]*)[\\|\s]*"'
regexNoUniqueNode = 'id_node: [\\|\s]*"([a-z0-9]*)[\\|\s]*"'

def node_query(id: str, version=None) -> SRNode | V2SRNode | None:
    if version is None or version == V1_SR_NODE_LABEL:
        query = SRNode.objects(id_node=id, document=V1_SR_NODE_LABEL)
        return None if query.count() == 0 else query.first()
    if version == V2_SR_NODE_LABEL:
        _id = valid_object_id(id)
        if _id is None:
            return None
        query = V2SRNode.objects(id=_id, document=version)
        if query.count() >0:
            node = query.first()
            return node
    return None

def valid_object_id(id:str)-> ObjectId | None:
    try:
        return ObjectId(id)
    except Exception as e:
        return None

def find_node_by_name_and_type(tipo: str, nombre: str, version=None) -> SRNode | V2SRNode | None:
    if version is None or version == V1_SR_NODE_LABEL:
        query = SRNode.objects(nombre=nombre, tipo=tipo, document=V1_SR_NODE_LABEL)
        return None if query.count() == 0 else query.first()
    if version == V2_SR_NODE_LABEL:
        query = V2SRNode.objects(nombre=nombre, tipo=tipo, document=version)
        return None if query.count() == 0 else query.first()
    return None

def find_node_by_id_entidad(id_entidad: str) -> SRNode | V2SRNode | None:
    query_sr_node = SRNode.objects(entidades__id_entidad=id_entidad)
    query_v2_sr_node = V2SRNode.objects(entidades__id_entidad=id_entidad)
    if query_sr_node.count() > 0:
        return query_sr_node.first()
    if query_v2_sr_node.count() > 0:
        return query_v2_sr_node.first()
    return None


def create_node(tipo:str, nombre: str, activado: bool, version=None) -> SRNode | V2SRNode | None:
    if version is None:
        return SRNode(nombre=nombre, tipo=tipo, activado=activado)
    if version == V2_SR_NODE_LABEL:
        return V2SRNode(nombre=nombre, tipo=tipo, activado=activado)
    return None

def create_instalation(instalacion_values: dict):
    instalacion = V2SRInstallation(**instalacion_values)
    success, msg = instalacion.save_safely()
    return success, msg, instalacion

def update_summary_node_info(node: SRNode | V2SRNode, summary: dict, replace = True) -> tuple[bool, str, SRNode | V2SRNode]:
    entidades = node.entidades if node.entidades is not None else list()
    for att in attributes_node:
        setattr(node, att, summary[att])
    node.update_node_id()
    to_change = summary.get(attr_entidades, None)
    if to_change is None:
        return True, "Todos lo cambios fueron hechos", node

    id_entities_lcl = [e[attr_id_entidad] for e in node.entidades]
    if replace:
        entidades = summary.get(attr_entidades) if isinstance(summary.get(attr_entidades, []), list) else list()
        id_entities_new = [e[attr_id_entidad] for e in entidades]
        ids_to_delete = [id for id in id_entities_lcl if id not in id_entities_new]
        check = [e[attr_entidad_tipo] + e[attr_entidad_nombre] for e in entidades]
        # check if there are repeated elements
        if len(check) > len(set(check)):
            return False, "Existen elementos repetidos dentro del nodo", node
        # delete those that are not in list coming from user interface
        [node.delete_entity_by_id(id) for id in ids_to_delete]
    # creación de nuevas entidades y actualización de valores
    new_entities = list()
    for i, entity in enumerate(entidades):
        if entity[attr_id_entidad] not in id_entities_lcl and isinstance(node, SRNode):
            e = SREntity(entidad_tipo=entity[attr_entidad_tipo], entidad_nombre=entity[attr_entidad_nombre])
        elif entity[attr_id_entidad] not in id_entities_lcl and isinstance(node, V2SRNode):
            e = V2SREntity(entidad_tipo=entity[attr_entidad_tipo], entidad_nombre=entity[attr_entidad_nombre])
        else:
            success, msg, e = node.search_entity_by_id(entity[attr_id_entidad])
            if not success:
                continue
        for att in attributes_entity:
            setattr(e, att, summary[attr_entidades][i][att])
            e.update_entity_id()
        new_entities.append(e)
    node.entidades = new_entities
    return True, "Todos lo cambios fueron hechos", node


def save_mongo_document_safely(document: Document, *args, **kwargs) -> tuple[bool, str]:
    try:
        document.save(*args, **kwargs)
        return True, 'Document saved successfully'
    except NotUniqueError as e:
        dup_key, collection = find_collection_and_dup_key(f'{e}')
        document_type = getattr(document, 'document', None)
        if (isinstance(document, SRNode) or isinstance(document, V2SRNode)
                or document_type == V1_SR_NODE_LABEL or document_type == V2_SR_NODE_LABEL):
            return msg_error_not_unique_node(document, e)

        return False, f"No unique key for: {dup_key} in {collection}"
    except Exception as e:
        return False, f"No able to save: {e}"

def msg_error_not_unique_node(node: SRNode | V2SRNode, exception: NotUniqueError) -> tuple[bool, str]:
    if 'entidades.id_entidad' in str(exception):
        group = re.findall(regexNoUniqueEntity, str(exception))
        id_entidad = group[0] if len(group) > 0 else ''
        success, msg, entidad = node.search_entity_by_id(id_entidad)
        node = find_node_by_id_entidad(id_entidad)
        return False, f"La entidad: {entidad} ya existe en nodo: {node} " if success else f'Conflicto con entidades no definido'
    if 'id_node' in str(exception):
        return False, f"El nodo ya existe: {node}"

    return False, f"No es posible crear el nodo: {node} debido a: \n{exception}"
