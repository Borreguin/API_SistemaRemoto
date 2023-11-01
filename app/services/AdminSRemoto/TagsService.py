from fastapi import UploadFile
from starlette import status

from app.common.util import to_dict, get_df_from_upload_file
from app.core.repositories import local_repositories
from app.schemas.RequestSchemas import TagListRequest, DeletedTagList, EditedListTagRequest
from app.utils.service_util import is_excel_file
from flask_app.dto.mongo_engine_handler.sRNode import SRNode, SRTag
from flask_app.my_lib.utils import find_entity_in_node, replace_edit_tags_in_node, get_df_from_excel_streamed_file, \
    check_if_all_are_there


def get_obtiene_lista_tags(id_nodo: str = "id nodo", id_entidad: str = "id entidad", id_utr: str = "id utr"):
    nodo = SRNode.objects(id_node=id_nodo).first()
    if nodo is None:
        return dict(success=False, msg="No se encuentra el nodo"), status.HTTP_404_NOT_FOUND
    success, idx = find_entity_in_node(nodo, id_entidad)
    if not success:
        return dict(success=False, msg="No se encuentra la entidad"), status.HTTP_404_NOT_FOUND

    for ix, _utr in enumerate(nodo.entidades[idx].utrs):
        if _utr.id_utr == id_utr or _utr.utr_code == id_utr:
            return dict(success=True, tags=[t.to_dict() for t in _utr.tags], msg="Tags encontradas"), status.HTTP_200_OK
    return dict(success=False, msg="No se encuentra la UTR"), status.HTTP_404_NOT_FOUND


def post_agrega_lista_de_tags_en_UTR(id_nodo: str = "id nodo", id_entidad: str = "id entidad",
                                     id_utr: str = "id utr", request_data: TagListRequest = None):
    request_dict = to_dict(request_data)
    nodo = SRNode.objects(id_node=id_nodo).first()
    if nodo is None:
        return dict(success=False, msg="No se encuentra el nodo"), status.HTTP_404_NOT_FOUND
    success, idx = find_entity_in_node(nodo, id_entidad)
    if not success:
        return dict(success=False, msg="No se encuentra la entidad"), status.HTTP_404_NOT_FOUND

    for ix, _utr in enumerate(nodo.entidades[idx].utrs):
        if _utr.id_utr == id_utr or _utr.utr_code == id_utr:
            SRTags, n_valid = list(), 0
            for tag in request_dict.get("tags", []):
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
                return dict(success=success, msg=msg, tags=tags), status.HTTP_200_OK
            if n_valid == 0:
                return dict(success=False, msg="No hay Tags válidas a ingresar"), status.HTTP_409_CONFLICT
            return dict(success=success, msg=msg), status.HTTP_409_CONFLICT
    return dict(success=False, msg="No se encuentra la UTR"), status.HTTP_404_NOT_FOUND


def put_edita_lista_de_tags_en_UTR(id_nodo: str, id_entidad: str, id_utr: str, request_data: EditedListTagRequest):
    request_dict = to_dict(request_data)
    nodo = SRNode.objects(id_node=id_nodo).first()
    if nodo is None:
        return dict(success=False, msg="No se encuentra el nodo"), status.HTTP_404_NOT_FOUND
    success, idx = find_entity_in_node(nodo, id_entidad)
    if not success:
        return dict(success=False, msg="No se encuentra la entidad"), status.HTTP_404_NOT_FOUND
    success, tags, msg = replace_edit_tags_in_node(nodo, idx, id_utr, request_dict.get('tags', []))
    return dict(success=success, tags=tags, msg=msg), status.HTTP_200_OK if success else 409


def delete_elimina_lista_de_tags_en_UTR(id_nodo: str, id_entidad: str, id_utr: str,
                                        request_data: DeletedTagList = None):
    nodo = SRNode.objects(id_node=id_nodo).first()
    if nodo is None:
        return dict(success=False, msg="No se encuentra el nodo"), status.HTTP_404_NOT_FOUND
    success, idx = find_entity_in_node(nodo, id_entidad)
    if not success:
        return dict(success=False, msg="No se encuentra la entidad"), status.HTTP_404_NOT_FOUND

    for ix, _utr in enumerate(nodo.entidades[idx].utrs):
        if _utr.id_utr == id_utr or _utr.utr_code == id_utr:
            tag_names = request_data.tags
            success, (n_remove, msg) = nodo.entidades[idx].utrs[ix].remove_tags(tag_names)
            if not success:
                return dict(success=success, msg=msg), status.HTTP_409_CONFLICT
            nodo.save()
            tags = [t.to_dict() for t in nodo.entidades[idx].utrs[ix].tags]
            return dict(success=success, msg=f"TAGS: -eliminadas: {n_remove} "
                                             f"-no encontradas: {len(tag_names) - n_remove}",
                        tags=tags), status.HTTP_200_OK

    return dict(success=False, msg="No se encuentra la UTR"), status.HTTP_404_NOT_FOUND


async def put_edita_lista_de_tags_en_UTR_usando_excel(id_nodo: str, id_entidad: str, id_utr: str,
                                                      excel_file: UploadFile):
    nodo = SRNode.objects(id_node=id_nodo).first()
    if nodo is None:
        return dict(success=False, msg="No se encuentra el nodo"), status.HTTP_404_NOT_FOUND
    success, idx = find_entity_in_node(nodo, id_entidad)
    if not success:
        return dict(success=False, msg="No se encuentra la entidad"), status.HTTP_404_NOT_FOUND

    if not is_excel_file(excel_file.content_type):
        return dict(success=False, msg="Formato Excel no admitido"), status.HTTP_400_BAD_REQUEST

    # reading file for reading tag changes:
    success, df_edited_tags, msg = await get_df_from_upload_file(excel_file, local_repositories.TEMPORAL)
    if not success:
        return dict(success=False, msg=msg), status.HTTP_409_CONFLICT
    columns = ['tag_name', 'filter_expression', 'activado', 'edited', 'tag_name_original']
    success, no_ok = check_if_all_are_there(columns, to_check=list(df_edited_tags.columns))
    if not success:
        return dict(success=False,
                    msg=f"Las siguientes columnas {no_ok} no existen en el archivo"), status.HTTP_400_BAD_REQUEST
    df_edited_tags = df_edited_tags[df_edited_tags["edited"] != '']
    df_edited_tags["activado"].fillna(False, inplace=True)
    tags_req = df_edited_tags[columns].to_dict(orient="records")
    # working with list of tags
    success, tags, msg = replace_edit_tags_in_node(nodo, idx, id_utr, tags_req)
    return dict(success=success, tags=tags, msg=msg), status.HTTP_200_OK if success else status.HTTP_409_CONFLICT
