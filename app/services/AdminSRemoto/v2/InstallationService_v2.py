from typing import Tuple

from starlette import status

from app.common.util import to_dict
from app.db.db_util import find_installation_by_id, find_node_by_id_entidad, save_mongo_document_safely
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


def put_installation(installation_id: str, request_data: InstallationRequest):
    installation = find_installation_by_id(installation_id)
    if installation is None:
        return dict(success=False, entidad=None, msg="Instalacion no encontrado"), status.HTTP_404_NOT_FOUND

    installation.update_from_dict(to_dict(request_data))
    success, msg = installation.save_safely()
    return dict(success=success, instalacion=installation.to_dict(),
                msg=msg), status.HTTP_200_OK if success else status.HTTP_304_NOT_MODIFIED


def delete_installation(entity_id: str, installation_id: str):
    installation = find_installation_by_id(installation_id)

    if installation is None:
        return dict(success=False, entidad=None, msg="Instalacion no encontrado"), status.HTTP_404_NOT_FOUND

    nodo = find_node_by_id_entidad(entity_id)
    if nodo is None:
        return dict(success=False, entidad=None, msg="No encontrado"), status.HTTP_404_NOT_FOUND

    success, msg, entity = nodo.search_entity_by_id(entity_id)
    if not success:
        return dict(success=False, entidad=None, msg=msg), status.HTTP_404_NOT_FOUND

    new_installations = [i for i in entity.instalaciones if str(i.pk) != installation_id]
    entity.instalaciones = new_installations

    success, msg = nodo.replace_entity_by_id(entity_id, entity)
    if not success:
        return dict(success=False, instalacion=None, msg=msg), status.HTTP_404_NOT_FOUND
    installation.delete()
    success_save, msg = nodo.save_safely()
    if not success_save:
            return dict(success=False, msg=msg), status.HTTP_304_NOT_MODIFIED
    return dict(success=True, msg=f'Instalación [{installation.instalacion_nombre}] eliminada'), status.HTTP_200_OK