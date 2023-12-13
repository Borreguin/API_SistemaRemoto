from __future__ import annotations

import os
from typing import Tuple

import pandas as pd
from mongoengine import Q
from starlette import status
from starlette.responses import FileResponse

from app.core.repositories import local_repositories
from app.db.db_util import get_v2sr_consignment
from app.db.v2.entities.v2_sRConsignment import V2SRConsignment
from app.db.v2.entities.v2_sRConsignments import V2SRConsignments
from app.schemas.RequestSchemas import FormatOption, V2ConsignmentDetailRequest
from app.utils.utils import check_range_yyyy_mm_dd_hh_mm_ss


def validate_ini_end_date_and_get_consignments(element_id: str,
                                               ini_date: str,
                                               end_date: str,
                                               create_if_not_exists=False
                                               ) -> Tuple[bool, str, V2SRConsignments | None]:
    success, ini_date, end_date, msg = check_range_yyyy_mm_dd_hh_mm_ss(ini_date, end_date)
    if not success:
        return False, msg, None
    query = V2SRConsignments.objects(element_id=element_id)
    if create_if_not_exists and query.count() == 0:
        return True, "Consignments created", V2SRConsignments(element_id=element_id, desde=ini_date, hasta=end_date)
    elif not create_if_not_exists and query.count() == 0:
        return False, "No existen consignaciones asociadas a este elemento", None
    return True, 'Consignment was found', query.first()


def v2_get_obtener_consignaciones_asociadas_elemento_id_por_fecha(element_id, ini_date: str, end_date: str):
    success, msg, consignments = validate_ini_end_date_and_get_consignments(element_id, ini_date, end_date)
    if not success:
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    consignaciones = consignments.consignments_in_time_range(ini_date, end_date)
    if len(consignaciones) == 0:
        return dict(success=False,
                    msg="No existen consignaciones en el periodo especificado"), status.HTTP_404_NOT_FOUND
    return dict(success=True, consignaciones=[c.to_dict() for c in consignaciones],
                msg="Se han encontrado consignaciones asociadas"), status.HTTP_200_OK


def v2_post_consignar_elemento_asociado_id_elemento(element_id: str, ini_date: str, end_date: str,
                                                    request_data: V2ConsignmentDetailRequest):
    """ Consignar un elemento asociadas a: "id_elemento"
        <b>id_elemento</b> corresponde al elemento a consignar
         formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
    """
    success, msg, consignments = validate_ini_end_date_and_get_consignments(element_id, ini_date, end_date,
                                                                            create_if_not_exists=True)
    if not success:
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    consignments.element_info = request_data.element_info
    consignments.save()

    consignment = V2SRConsignment(no_consignacion=request_data.no_consignacion, fecha_inicio=ini_date,
                                  fecha_final=end_date, responsable=request_data.responsable,
                                  element_info=request_data.element_info)

    # ingresando consignación y guardando si es exitoso:
    success, msg = consignments.insert_consignment(consignment)
    if success:
        consignments.save()
    return dict(success=success, msg=msg), status.HTTP_200_OK if success else status.HTTP_400_BAD_REQUEST


def v2_delete_elimina_consignacion_por_id_elemento_id_consignacion(element_id, consignment_id):
    query = V2SRConsignments.objects(id_elemento=element_id)
    if query.count() == 0:
        return dict(success=False, msg="No existen consignaciones para este elemento. "
                                       "El elemento no existe"), status.HTTP_404_NOT_FOUND

    consignments = query.first()
    # eliminando consignación por id
    success, msg = consignments.delete_consignment_by_id(consignment_id)
    if success:
        consignments.save()
    return dict(success=success, msg=msg), status.HTTP_200_OK if success else status.HTTP_404_NOT_FOUND


def v2_put_edita_consignacion_por_elemento_id_y_consignacion_id(element_id: str,
                                                                consignment_id: str,
                                                                request_data: V2ConsignmentDetailRequest):
    query = V2SRConsignments.objects(element_id=element_id)
    if query.count() == 0:
        return dict(success=False,
                    msg="No existen consignaciones para este elemento. El elemento no existe"), status.HTTP_404_NOT_FOUND
    consignments = query.first()
    success, msg = consignments.delete_consignment_by_id(consignment_id)
    if not success:
        return dict(success=success, msg=msg), status.HTTP_404_NOT_FOUND

    consignment = V2SRConsignment(no_consignacion=request_data.no_consignacion, fecha_inicio=request_data.fecha_inicio,
                                  fecha_final=request_data.fecha_final, responsable=request_data.responsable,
                                  element_info=request_data.element_info)

    # ingresando consignación y guardando si es exitoso:
    success, msg = consignments.insert_consignment(consignment)
    if success:
        consignments.save()
    return dict(success=success, msg=msg), status.HTTP_200_OK if success else status.HTTP_400_BAD_REQUEST


async def v2_post_carga_archivo_a_consignacion_asociada_al_elemento(element_id: str, consignment_id: str, upload_file=None):
    """
        Carga un archivo a la consignación asociada al elemento: "element_id", cuya idenficación es "consignment_id" </br>
        <b>element_id</b> corresponde al elemento consignado   </br>
        <b>consignment_id</b> corresponde a la identificación de la consignación </br>
        formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
    """
    consignments = get_v2sr_consignment(element_id)
    if consignments is None:
        return dict(success=False, msg="No existen consignaciones para este elemento"), status.HTTP_404_NOT_FOUND

    success, consignment = consignments.search_consignment_by_id(id_to_search=consignment_id)
    if not success:
        return dict(success=False, msg="No existe consignación para este elemento"), status.HTTP_404_NOT_FOUND
    consignment.create_folder()
    filename = upload_file.filename
    destination = os.path.join(local_repositories.CONSIGNMENTS, consignment_id, filename)
    with open(destination, 'wb') as f:
        f.write(await upload_file.read())
    consignments.save()
    return dict(success=True, msg="Documento cargado exitosamente"), status.HTTP_200_OK


def v2_get_obtener_consignaciones_en_rango_fecha(formato: FormatOption, ini_date: str, end_date: str):
    """ Obtener las consignaciones existentes en las fechas consultadas
        formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
        formatos disponibles: [json, excel]
    """
    success, ini_date, end_date, msg = check_range_yyyy_mm_dd_hh_mm_ss(ini_date, end_date)
    if not success:
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST

    #   D*****[***************H
    contiene_ini = Q(desde__lte=ini_date) & Q(hasta__gte=ini_date)
    #   D*****]***************H
    contiene_end = Q(desde__lte=end_date) & Q(hasta__gte=end_date)
    #   [     D********H    ]
    contenido_en = Q(desde__gte=ini_date) & Q(hasta__lte=end_date)
    time_query = contiene_ini | contiene_end | contenido_en
    consignments = V2SRConsignments.objects.filter(time_query)

    if len(consignments) == 0:
        return dict(success=False,
                    msg=f"No se encontraron consignaciones para [{ini_date} @ {end_date}]"), status.HTTP_404_NOT_FOUND
    consignment_result = list()
    for consignment in consignments:
        consignment_dicts = consignment.consignments_in_time_range(ini_date, end_date)
        consignment_result += consignment_dicts
    if len(consignment_result) == 0:
        return dict(success=False,
                    msg=f"No se encontraron consignaciones para [{ini_date} @ {end_date}]"), status.HTTP_404_NOT_FOUND

    if formato == FormatOption.JSON:
        return dict(success=True, consignaciones=consignment_result), status.HTTP_200_OK

    ini_date_str, end_date_str = ini_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    file_name = f"Consignaciones_{ini_date_str}@{end_date_str}.xlsx"
    path = os.path.join(local_repositories.TEMPORAL, file_name)
    with pd.ExcelWriter(path) as writer:
        df_consignment = pd.DataFrame(consignment_result)
        df_consignment.to_excel(writer, sheet_name="consignaciones")
    if os.path.exists(path):
        # resp = send_from_directory(os.path.dirname(path), file_name, as_attachment=True)
        # return set_max_age_to_response(resp, 30)
        return FileResponse(path=path, filename=file_name, media_type='application/octet-stream',
                            content_disposition_type="attachment")
    return dict(success=False, consignaciones=consignment_result), status.HTTP_404_NOT_FOUND
