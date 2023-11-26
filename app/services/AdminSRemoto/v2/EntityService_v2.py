from typing import Tuple

from starlette import status

from app.db.util import find_node_by_id_entidad


def get_entity_by_id(entity_id: str) -> Tuple[dict, int]:
    nodo = find_node_by_id_entidad(entity_id)
    if nodo is None:
        return dict(success=False, entidad=None, msg="No encontrado"), status.HTTP_404_NOT_FOUND
    success, msg, entity = nodo.search_entity_by_id(entity_id)
    if not success:
        return dict(success=False, entidad=entity, msg=msg), status.HTTP_404_NOT_FOUND
    return dict(success=True, entidad=entity.to_dict(), msg="Encontrado"), status.HTTP_200_OK