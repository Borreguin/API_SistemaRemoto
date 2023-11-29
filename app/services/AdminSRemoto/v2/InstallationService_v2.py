from typing import Tuple

from starlette import status

from app.common.util import to_dict
from app.db.util import find_node_by_id_entidad, find_installation_by_id
from app.db.v2.entities.v2_sRInstallation import V2SRInstallation
from app.schemas.RequestSchemas import InstallationRequest


def get_installation_by_id(installation_id: str) -> Tuple[dict, int]:
    installation = find_installation_by_id(installation_id)
    if installation is None:
        return dict(success=False, entidad=None, msg="No encontrado"), status.HTTP_404_NOT_FOUND
    return dict(success=True, entidad=installation.to_dict(), msg="Encontrado"), status.HTTP_200_OK


def post_installation(entidad_id: str, request_data: InstallationRequest):
    new_installation = V2SRInstallation(**to_dict(request_data))
    success, msg = new_installation.save_safely()
    if not success:
        return dict(success=False, instalacion=None, msg=msg), status.HTTP_409_CONFLICT
    nodo = find_node_by_id_entidad(entidad_id)
    success, msg, entity = nodo.search_entity_by_id(entidad_id)
    if not success:
        return dict(success=False, instalacion=None, msg=msg), status.HTTP_404_NOT_FOUND
    entity.instalaciones.append(new_installation)
    success, msg = nodo.replace_entity_by_id(entidad_id, entity)
    if not success:
        return dict(success=False, instalacion=None, msg=msg), status.HTTP_404_NOT_FOUND
    success, msg = nodo.save_safely()
    return dict(success=success, instalacion=new_installation.to_dict(),
                msg=msg), status.HTTP_200_OK if success else status.HTTP_304_NOT_MODIFIED
