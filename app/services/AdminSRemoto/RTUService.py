import math

from starlette import status

from app.common.util import to_dict
from app.schemas.RequestSchemas import RTURequest, RTURequestId
from flask_app.dto.mongo_engine_handler.sRNode import SRNode, SRUTR
from flask_app.my_lib.utils import find_entity_in_node


def get_lista_RTU_de_entidad(id_nodo: str = "id nodo", id_entidad: str = "id entidad"):
    nodo = SRNode.objects(id_node=id_nodo).as_pymongo().first()
    if nodo is None:
        return dict(success=False, msg="No se encuentra el nodo"), status.HTTP_404_NOT_FOUND
    success, idx = find_entity_in_node(nodo, id_entidad)
    if not success:
        return dict(success=False, msg="No se encuentra la entidad"), status.HTTP_404_NOT_FOUND

    utrs_list = nodo["entidades"][idx]["utrs"]
    for utr in utrs_list:
        utr.pop('consignaciones', None)
        utr.pop("tags", None)
        utr['longitude'] = utr['longitude'] \
            if 'longitude' in utr.keys() and not math.isnan(utr['longitude']) else 0
        utr['latitude'] = utr['latitude'] \
            if 'latitude' in utr.keys() and not math.isnan(utr['latitude']) else 0
    utrs_list = sorted(utrs_list, key=lambda i: i['utr_nombre'])
    return dict(success=True, utrs=utrs_list), status.HTTP_200_OK


def post_ingresa_nueva_RTU_en_entidad(id_nodo: str, id_entidad: str, request_data: RTURequest):

    nodo = SRNode.objects(id_node=id_nodo).first()
    if nodo is None:
        return dict(success=False, msg="No se encuentra el nodo"), status.HTTP_404_NOT_FOUND
    success, idx = find_entity_in_node(nodo, id_entidad)
    if not success:
        return dict(success=False, msg="No se encuentra la entidad"), status.HTTP_404_NOT_FOUND
    rtu = SRUTR(**to_dict(request_data))
    success, msg = nodo.entidades[idx].add_or_rename_utrs([rtu])
    utrs = nodo.entidades[idx].utrs
    if success:
        # rtu.create_consignments_container()
        nodo.save()
        return dict(success=success, utrs=[u.to_dict() for u in utrs], msg=msg), status.HTTP_200_OK
    else:
        return dict(success=success, utrs=[u.to_dict() for u in utrs], msg=msg), status.HTTP_409_CONFLICT


def delete_elimina_RTU_en_entidad(id_nodo: str, id_entidad: str, request_data: RTURequestId):

    nodo = SRNode.objects(id_node=id_nodo).first()
    if nodo is None:
        return dict(success=False, msg="No se encuentra el nodo"), status.HTTP_404_NOT_FOUND
    success, idx = find_entity_in_node(nodo, id_entidad)
    if not success:
        return dict(success=False, msg="No se encuentra la entidad"), status.HTTP_404_NOT_FOUND
    success, msg = nodo.entidades[idx].remove_utrs([request_data.id_utr])
    if success:
        nodo.save()
        return dict(success=success, msg=msg, utrs=[r.to_dict() for r in nodo.entidades[idx].utrs]), status.HTTP_200_OK
    else:
        return dict(success=success, msg=msg), status.HTTP_409_CONFLICT


def get_obtiene_configuracion_RTU(id_nodo: str, id_entidad: str, id_utr: str):
    nodo = SRNode.objects(id_node=id_nodo).first()
    if nodo is None:
        return dict(success=False, msg="No se encuentra el nodo"), status.HTTP_404_NOT_FOUND
    success, idx = find_entity_in_node(nodo, id_entidad)
    if not success:
        return dict(success=False, msg="No se encuentra la entidad"), status.HTTP_404_NOT_FOUND

    for ix, _utr in enumerate(nodo.entidades[idx].utrs):
        if _utr.id_utr == id_utr or _utr.utr_code == id_utr:
            return dict(success=True, msg="RTU encontrada", utr=_utr.to_dict()), status.HTTP_200_OK

    return dict(success=False, msg="RTU no encontrada"), status.HTTP_404_NOT_FOUND
