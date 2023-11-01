import os

from mongoengine import Q
from starlette import status
from starlette.responses import FileResponse

from app.core.repositories import local_repositories
from app.schemas.RequestSchemas import DetalleConsignacionRequest, ConsignacionRequest, FormatOption
from flask_app.dto.mongo_engine_handler.Info.Consignment import Consignments, Consignment
from flask_app.my_lib.utils import check_range_yyyy_mm_dd_hh_mm_ss, set_max_age_to_response
import pandas as pd


def get_obtener_consignaciones_asociadas_elemento_id_por_fecha(id_elemento, ini_date, end_date):
    success, ini_date, end_date, msg = check_range_yyyy_mm_dd_hh_mm_ss(ini_date, end_date)
    if not success:
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST

    consignacion = Consignments.objects(id_elemento=id_elemento).first()
    if consignacion is None:
        return dict(success=False, msg="No existen consignaciones asociadas a este elemento"), status.HTTP_404_NOT_FOUND
    consignaciones = consignacion.consignments_in_time_range(ini_date, end_date)
    if len(consignaciones) == 0:
        return dict(success=False,
                    msg="No existen consignaciones en el periodo especificado"), status.HTTP_404_NOT_FOUND
    return dict(success=True, consignaciones=[c.to_dict() for c in consignaciones],
                msg="Se han encontrado consignaciones asociadas"), status.HTTP_200_OK


def post_consignar_elemento_asociado_id_elemento(id_elemento: str, ini_date: str, end_date: str,
                                                 request_data: DetalleConsignacionRequest):
    """ Consignar un elemento asociadas a: "id_elemento"
        <b>id_elemento</b> corresponde al elemento a consignar
         formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
    """
    success, ini_date, end_date, msg = check_range_yyyy_mm_dd_hh_mm_ss(ini_date, end_date)
    if not success:
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST

    if ini_date >= end_date:
        msg = "El rango de fechas es incorrecto. Revise que la fecha inicial sea anterior a fecha final"
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    consignaciones = Consignments.objects(id_elemento=id_elemento).first()
    if consignaciones is None:
        consignaciones = Consignments(id_elemento=id_elemento)
    # Actualizando el elemento referenciado:
    if request_data.elemento is not None:
        consignaciones.elemento = request_data.elemento
    consignacion = Consignment(no_consignacion=request_data.no_consignacion, fecha_inicio=ini_date,
                               fecha_final=end_date, detalle=request_data.detalle, responsable=request_data.responsable)
    # ingresando consignación y guardando si es exitoso:
    success, msg = consignaciones.insert_consignments(consignacion)
    if success:
        consignaciones.save()
        return dict(success=success, msg=msg), status.HTTP_200_OK
    else:
        return dict(success=success, msg=msg), status.HTTP_200_OK


def delete_elimina_consignacion_por_id_elemento_id_consignacion(id_elemento, id_consignacion):
    consignaciones = Consignments.objects(id_elemento=id_elemento).first()
    if consignaciones is None:
        return dict(success=False, msg="No existen consignaciones para este elemento. "
                                       "El elemento no existe"), status.HTTP_404_NOT_FOUND

    # eliminando consignación por id
    success, msg = consignaciones.delete_consignment_by_id(id_consignacion)
    if success:
        consignaciones.save()
        return dict(success=success, msg=msg), status.HTTP_200_OK
    else:
        return dict(success=success, msg=msg), status.HTTP_200_OK


def put_edita_consignacion_por_elemento_id_y_consignacion_id(id_elemento: str = "id_elemento",
                                                             id_consignacion: str = "id_consignacion",
                                                             request_data: ConsignacionRequest = ConsignacionRequest()):
    request_dict = request_data.__dict__
    elemento = request_dict.pop("elemento", None)
    consignaciones = Consignments.objects(id_elemento=id_elemento).first()
    if consignaciones is None:
        return dict(success=False, msg="No existen consignaciones para este elemento. "
                                       "El elemento no existe"), status.HTTP_404_NOT_FOUND

    if elemento is not None:
        consignaciones.elemento = elemento

    # editando consignación por id
    consignacion = Consignment(**request_dict)
    success, msg = consignaciones.edit_consignment_by_id(id_to_edit=id_consignacion, consignment=consignacion)
    if success:
        consignaciones.save()
        return dict(success=success, msg=msg), status.HTTP_200_OK
    else:
        return dict(success=success, msg=msg), status.HTTP_404_NOT_FOUND


async def post_carga_archivo_a_consignacion_asociada_al_elemento(id_elemento: str = "id_elemento",
                                                                 id_consignacion: str = "id_consignacion",
                                                                 upload_file=None):
    """ Carga un archivo a la consignación asociada al elemento: "id_elemento", cuya idenficación es "id_consignacion"
                <b>id_elemento</b> corresponde al elemento consignado
                <b>id_consignacion</b> corresponde a la identificación de la consignación
                formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
    """
    consignaciones = Consignments.objects(id_elemento=id_elemento).first()
    if consignaciones is None:
        return dict(success=False, msg="No existen consignaciones para este elemento. "
                                       "El elemento no existe"), status.HTTP_404_NOT_FOUND
    success, consignacion = consignaciones.search_consignment_by_id(id_to_search=id_consignacion)
    if not success:
        return dict(success=False, msg="No existe consignación para este elemento"), status.HTTP_404_NOT_FOUND
    consignacion.create_folder()
    filename = upload_file.filename
    destination = os.path.join(local_repositories.CONSIGNMENTS, id_consignacion, filename)
    with open(destination, 'wb') as f:
        f.write(await upload_file.read())
    consignaciones.save()
    return dict(success=True, msg="Documento cargado exitosamente"), status.HTTP_200_OK


def get_obtener_consignaciones_en_rango_fecha(formato: FormatOption, ini_date: str, end_date: str):
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
    consignaciones = Consignments.objects.filter(time_query)

    if len(consignaciones) == 0:
        return dict(success=False,
                    msg=f"No se encontraron consignaciones para [{ini_date} @ {end_date}]"), status.HTTP_404_NOT_FOUND
    consignment_result = list()
    for c_consignacion in consignaciones:
        consignment_dicts = c_consignacion.consignments_in_time_range_w_element(ini_date, end_date)
        consignment_result += consignment_dicts
    if len(consignment_result) == 0:
        return dict(success=False,
                    msg=f"No se encontraron consignaciones para [{ini_date} @ {end_date}]"), status.HTTP_404_NOT_FOUND

    if format == 'json':
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
