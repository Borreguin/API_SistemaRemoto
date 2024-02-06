from starlette import status

from app.db.db_util import find_installation_by_id
from app.db.v2.entities.v2_sRBahia import V2SRBahia


def get_bahia_by_id(installation_id: str, document_id: str):
    installation = find_installation_by_id(installation_id)
    if installation is None:
        return dict(success=False, bahia=None, msg="Instalación no encontrado"), status.HTTP_404_NOT_FOUND
    bahia = installation.find_bahia_by_id(document_id)
    if bahia is None:
        return dict(success=False, bahia=None, msg="Bahía no encontrado"), status.HTTP_404_NOT_FOUND
    return dict(success=True, bahia=bahia.to_dict(), msg="Bahía encontrado"), status.HTTP_200_OK


def create_bahia(installation_id: str, bahia: dict):
    installation = find_installation_by_id(installation_id)
    if installation is None:
        return dict(success=False, bahia=None, msg="Instalación no encontrado"), status.HTTP_404_NOT_FOUND
    success, msg = installation.add_bahia(V2SRBahia(**bahia))
    installation.save_safely()
    return dict(success=success, bahia=bahia, msg=msg), status.HTTP_200_OK if success else status.HTTP_406_NOT_ACCEPTABLE


def update_bahia(installation_id: str, bahia_id: str, bahia: dict):
    installation = find_installation_by_id(installation_id)
    if installation is None:
        return dict(success=False, bahia=None, msg="Instalación no encontrado"), status.HTTP_404_NOT_FOUND
    success, msg = installation.update_bahia(bahia_id, V2SRBahia(**bahia))
    return dict(success=success, bahia=bahia, msg=msg), status.HTTP_200_OK if success else status.HTTP_422_UNPROCESSABLE_ENTITY


def delete_bahia(installation_id: str, document_id: str):
    installation = find_installation_by_id(installation_id)
    if installation is None:
        return dict(success=False, bahia=None, msg="Instalación no encontrado"), status.HTTP_404_NOT_FOUND
    success, msg = installation.remove_bahia(V2SRBahia(document_id=document_id))
    return dict(success=success, bahia=None, msg=msg), status.HTTP_200_OK if success else status.HTTP_422_UNPROCESSABLE_ENTITY
