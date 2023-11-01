import math
import re

from starlette import status

from app.common.util import to_dict
from app.schemas.RequestSchemas import *
from app.utils.service_util import *
from flask_app.dto.mongo_engine_handler.sRNode import SRNode, SREntity


def put_activa_nodo(id_nodo: str = "ID del nodo a cambiar") -> Tuple[dict, int]:
    nodo = SRNode.objects(id_node=id_nodo).first()
    if nodo is None:
        return dict(success=False, nodo=None, msg="No encontrado"), status.HTTP_404_NOT_FOUND
    nodo.actualizado, nodo.activado = dt.datetime.now(), True
    nodo.save()
    return dict(success=True, nodo=nodo.to_dict(), msg="Nodo activado"), status.HTTP_200_OK


def put_desactiva_nodo(id_nodo: str = "ID del nodo a cambiar") -> Tuple[dict, int]:
    nodo = SRNode.objects(id_node=id_nodo).first()
    if nodo is None:
        return dict(success=False, nodo=None, msg="No encontrado"), status.HTTP_404_NOT_FOUND
    nodo.actualizado, nodo.activado = dt.datetime.now(), False
    nodo.save()
    return dict(success=True, nodo=nodo.to_dict(), msg="Nodo desactivado"), status.HTTP_200_OK


def delete_elimina_nodo_usando_ID_como_referencia(id: str) -> Tuple[dict, int]:
    node = SRNode.objects(id_node=id).first()
    if node is None:
        return dict(success=False, nodo=None, msg="No se encontró el nodo"), status.HTTP_404_NOT_FOUND
    node.delete()
    return dict(success=True, nodo=node.to_dict(), msg="Nodo eliminado"), status.HTTP_200_OK


def put_actualiza_cambios_menores_en_nodo(id: str, request_data: BasicNodeInfoRequest) -> Tuple[dict, int]:
    node = SRNode.objects(id_node=id).first()
    if node is None:
        return dict(success=False, nodo=None, msg=f"No se encontró el nodo {id}"), status.HTTP_404_NOT_FOUND
    success, msg = node.update_summary_info(to_dict(request_data))
    if not success:
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    node.save()
    return dict(success=True, nodo=node.to_summary(),
                msg=f"Se han guardado los cambios para nodo {id}"), status.HTTP_200_OK


def post_crea_nuevo_nodo_usando_ID(id, request_data: BasicNodeInfoRequest) -> Tuple[dict, int]:
    nodo = SRNode.objects(id_node=id).first()
    if nodo is not None:
        return dict(success=False, msg="El nodo ya existe, no puede ser creado"), status.HTTP_400_BAD_REQUEST
    nodo = SRNode(nombre=request_data.nombre, tipo=request_data.tipo, activado=request_data.activado)
    success, msg = nodo.update_summary_info(to_dict(request_data))
    if not success:
        return dict(success=False, nodo=None, msg=msg), status.HTTP_400_BAD_REQUEST
    try:
        nodo.save()
    except Exception as e:
        # problema al existir entidad nula en un nodo ya existente
        if "entidades.id_entidad_1 dup key" in str(e):
            entity = SREntity(entidad_nombre="Nombre " + str(randint(0, 1000)), entidad_tipo="Empresa")
            nodo.add_or_replace_entities([entity])
            nodo.save()
    return dict(success=True, nodo=nodo.to_summary(), msg="Nodo creado"), status.HTTP_200_OK


def get_retorna_entidades_de_nodo(tipo: str, nombre: str, entidad_tipo: str, entidad_nombre: str):
    nodo = SRNode.objects(nombre=nombre, tipo=tipo).first()
    if nodo is None:
        return nodo, status.HTTP_404_NOT_FOUND
    for entidad in nodo.entidades:
        if entidad.entidad_tipo == entidad_tipo and entidad.entidad_nombre == entidad_nombre:
            return entidad.to_dict(), status.HTTP_200_OK
    return None, status.HTTP_404_NOT_FOUND


def get_muestra_todos_los_nombres_nodos_existentes(filter_str=None):

    nodes = SRNode.objects().as_pymongo().exclude('id')
    if nodes.count() == 0:
        return dict(success=False, msg=f"No hay nodos en la base de datos"), status.HTTP_404_NOT_FOUND
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
                entidades.append(entidad)
            # creando el resumen del nodo
            node["actualizado"] = str(node["actualizado"])
            node["entidades"] = entidades
            _nodes.append(node)
        # to_show = [n.to_summary() for n in nodes]
        return dict(success=True, nodos=_nodes, msg=f"Se han obtenido {len(_nodes)} nodos"), status.HTTP_200_OK
    filter_str = str(filter_str).replace("*", ".*")
    regex = re.compile(filter_str, re.IGNORECASE)
    nodes = SRNode.objects(nombre=regex)
    to_show = [n.to_summary() for n in nodes]
    return dict(success=True, nodos=to_show, msg=f"Se han obtenido {len(to_show)} nodos"), status.HTTP_200_OK

