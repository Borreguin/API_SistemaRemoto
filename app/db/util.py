from __future__ import annotations

from app.db.constants import attributes_node, attr_id_entidad, attr_entidades, attr_entidad_tipo, attr_entidad_nombre, \
    attributes_entity, V2_SR_NODE_LABEL, V1_SR_NODE_LABEL
from app.db.v1.sRNode import SRNode, SREntity
from app.db.v2.entities.v2_sREntity import V2SREntity
from app.db.v2.entities.v2_sRNode import V2SRNode

def node_query(id: str, version=None) -> SRNode | V2SRNode | None:
    if version is None:
        query = SRNode.objects(id_node=id, version=V1_SR_NODE_LABEL)
        return None if query.count() == 0 else query.first()
    if version == V2_SR_NODE_LABEL:
        query = V2SRNode.objects(id_node=id, version=version)
        return None if query.count() == 0 else query.first()
    return None

def find_node_by_name_and_type(tipo: str, nombre: str, version=None) -> SRNode | V2SRNode | None:
    if version is None:
        query = SRNode.objects(nombre=nombre, tipo=tipo, version=V1_SR_NODE_LABEL)
        return None if query.count() == 0 else query.first()
    if version == V2_SR_NODE_LABEL:
        query = V2SRNode.objects(nombre=nombre, tipo=tipo, version=version)
        return None if query.count() == 0 else query.first()
    return None

def create_node(tipo:str, nombre: str, activado: bool, version=None) -> SRNode | V2SRNode | None:
    if version is None:
        return SRNode(nombre=nombre, tipo=tipo, activado=activado)
    if version == V2_SR_NODE_LABEL:
        return V2SRNode(nombre=nombre, tipo=tipo, activado=activado)
    return None


def update_summary_node_info(node: SRNode | V2SRNode, summary: dict) -> tuple[bool, str, SRNode | V2SRNode]:
    for att in attributes_node:
        setattr(node, att, summary[att])
    id_entities_lcl = [e[attr_id_entidad] for e in node.entidades]
    id_entities_new = [e[attr_id_entidad] for e in summary.get(attr_entidades, [])]
    ids_to_delete = [id for id in id_entities_lcl if id not in id_entities_new]
    check = [e[attr_entidad_tipo] + e[attr_entidad_nombre] for e in summary[attr_entidades]]
    # check if there are repeated elements
    if len(check) > len(set(check)):
        return False, "Existen elementos repetidos dentro del nodo", node
    # delete those that are not in list coming from user interface
    [node.delete_entity_by_id(id) for id in ids_to_delete]
    # creación de nuevas entidades y actualización de valores
    new_entities = list()
    for i, entity in enumerate(summary[attr_entidades]):
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
        new_entities.append(e)
    node.entidades = new_entities
    return True, "Todos lo cambios fueron hechos", node