
from starlette import status

from app.db.db_util import get_temporal_status, get_node_details_report_by_id_node
from app.db.v2.entities.v2_sRNode import V2SRNode
from app.db.v2.v2SRNodeReport.report_util import get_report_id
from app.schemas.RequestSchemas import NodeRequest, NodesRequest
from app.utils.service_util import get_sr_final_report, check_if_report_is_in_progress
from app.utils.utils import isTemporal, \
    check_range_yyyy_mm_dd_hh_mm_ss
from flask_app.motor.master_scripts.eng_sRmaster import *


def put_calcula_o_sobreescribe_disponibilidad_en_rango_fecha(ini_date, end_date):
    from app.core.v2CalculationEngine.master.master import run_all_active_nodes
    success, ini_date, end_date, msg = check_range_yyyy_mm_dd_hh_mm_ss(ini_date, end_date)
    if not success:
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    is_permanent = not isTemporal(ini_date, end_date)
    success, msg, report_id = run_all_active_nodes(ini_date, end_date, force=True, permanent_report=is_permanent)
    return dict(success=success, msg=msg, report_id=report_id), status.HTTP_200_OK if success else 409


def post_calcula_disponibilidad_en_rango_fecha(ini_date, end_date):
    from app.core.v2CalculationEngine.master.master import run_all_active_nodes
    success, ini_date, end_date, msg = check_range_yyyy_mm_dd_hh_mm_ss(ini_date, end_date)
    if not success:
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    is_permanent = not isTemporal(ini_date, end_date)
    success, msg, report_id= run_all_active_nodes(ini_date, end_date, force=False, permanent_report=is_permanent)
    return dict(success=success, msg=msg, report_id=report_id), status.HTTP_200_OK if success else 409


def get_obtiene_disponibilidad_en_rango_fecha(ini_date: str = "yyyy-mm-dd H:M:S", end_date: str = "yyyy-mm-dd H:M:S"):
    success, ini_date, end_date, msg = check_range_yyyy_mm_dd_hh_mm_ss(ini_date, end_date)
    if not success:
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    is_permanent = not isTemporal(ini_date, end_date)
    status_report = check_if_report_is_in_progress(ini_date, end_date, is_permanent)
    if status_report is not None and status_report.processing:
        return dict(success=False, msg=f"Hay un cálculo que se encuentra en progreso, iniciado al {status_report.created}, vuelva a intentar mirar su estado en 5 min."), status.HTTP_409_CONFLICT

    final_report = get_sr_final_report(ini_date, end_date, is_permanent=is_permanent)
    if final_report is None:
        return dict(success=False, msg="No existe reporte asociado"), status.HTTP_404_NOT_FOUND
    return dict(success=True, report=final_report.to_dict()), status.HTTP_200_OK


def put_calcula_o_sobrescribe_disponibilidad_nodo_en_rango_fecha(ini_date: str = "yyyy-mm-dd H:M:S",
                                                                 end_date: str = "yyyy-mm-dd H:M:S",
                                                                 request_data: NodeRequest = NodeRequest()):
    success, ini_date, end_date, msg = check_range_yyyy_mm_dd_hh_mm_ss(ini_date, end_date)
    if not success:
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST

    node_list_name = [request_data.nombre]
    success1, result, msg1 = run_node_list(node_list_name, ini_date, end_date, save_in_db=True, force=True)
    if success1:
        success2, report, msg2 = run_summary(ini_date, end_date, save_in_db=True, force=True,
                                             results=result, log_msg=msg1)
        if success2:
            return dict(result=result, msg=msg1, report=report.to_dict()), status.HTTP_200_OK
        else:
            return dict(success=False, msg=msg2), status.HTTP_409_CONFLICT
    elif not success1:
        return dict(success=False, msg=result), status.HTTP_409_CONFLICT


def put_calcula_o_sobrescribe_disponibilidad_nodos_en_lista(ini_date: str = "yyyy-mm-dd H:M:S",
                                                            end_date: str = "yyyy-mm-dd H:M:S",
                                                            request_data: NodesRequest = NodesRequest()):
    from app.core.v2CalculationEngine.master.master import overwrite_node_reports_by_node_ids
    success, ini_date, end_date, msg = check_range_yyyy_mm_dd_hh_mm_ss(ini_date, end_date)
    if not success:
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST

    is_permanent = not isTemporal(ini_date, end_date)
    success, msg, report_id = overwrite_node_reports_by_node_ids(request_data.nodos, ini_date, end_date, permanent_report=is_permanent)
    return dict(success=success, msg=msg, report_id=report_id), status.HTTP_200_OK if success else 409


def post_calcula_disponibilidad_nodos_en_lista(ini_date: str = "yyyy-mm-dd H:M:S", end_date: str = "yyyy-mm-dd H:M:S",
                                               request_data: NodesRequest = NodesRequest()):
    success, ini_date, end_date, msg = check_range_yyyy_mm_dd_hh_mm_ss(ini_date, end_date)
    if not success:
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST

    success1, result, msg1 = run_node_list(request_data.nodos, ini_date, end_date, save_in_db=True, force=False)
    not_calculated = [True for k in result.keys() if "No ha sido calculado" in result[k]]
    if all(not_calculated) and len(not_calculated) > 0:
        return dict(success=False, msg="No ha sido calculado, ya existe en base de datos")
    if success1:
        success2, report, msg2 = run_summary(ini_date, end_date, save_in_db=True, force=True,
                                             results=result, log_msg=msg1)
        if success2:
            return dict(result=result, msg=msg1, report=report.to_dict()), status.HTTP_200_OK
        else:
            return dict(success=False, msg=msg2), status.HTTP_409_CONFLICT
    elif not success1:
        return dict(success=False, msg=result), status.HTTP_409_CONFLICT


def delete_elimina_disponibilidad_de_nodos_en_lista(ini_date: str = "yyyy-mm-dd H:M:S",
                                                    end_date: str = "yyyy-mm-dd H:M:S",
                                                    request_data: NodesRequest = NodesRequest()):
    from app.core.v2CalculationEngine.master.master import delete_node_reports_by_node_ids
    success, ini_date, end_date, msg = check_range_yyyy_mm_dd_hh_mm_ss(ini_date, end_date)
    if not success:
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    is_permanent = not isTemporal(ini_date, end_date)
    success, msg, report_id = delete_node_reports_by_node_ids(request_data.nodos, ini_date, end_date, permanent_report=is_permanent)
    return dict(success=success, msg=msg, report_id=report_id), status.HTTP_200_OK if success else 409

def get_obtiene_disponibilidad_por_tipo_nodo(tipo="tipo de nodo", nombre="nombre del nodo",
                                             ini_date: str = "yyyy-mm-dd H:M:S",
                                             end_date: str = "yyyy-mm-dd H:M:S"):
    success, ini_date, end_date, msg = check_range_yyyy_mm_dd_hh_mm_ss(ini_date, end_date)
    if not success:
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    v_report = SRNodeDetailsBase(tipo=tipo, nombre=nombre, fecha_inicio=ini_date, fecha_final=end_date)
    if isTemporal(ini_date, end_date):
        report = SRNodeDetailsTemporal.objects(id_report=v_report.id_report).first()
    else:
        report = SRNodeDetailsPermanente.objects(id_report=v_report.id_report).first()
    if report is None:
        return dict(success=False,
                    msg="No existe el cálculo para este nodo en la fecha indicada"), status.HTTP_404_NOT_FOUND
    else:
        return dict(success=True, report=report.to_dict(), msg='Reporte enconetrado'), status.HTTP_200_OK


def delete_elimina_reporte_disponibilidad_de_nodo(tipo="tipo de nodo", nombre="nombre del nodo",
                                                  ini_date: str = "yyyy-mm-dd H:M:S",
                                                  end_date: str = "yyyy-mm-dd H:M:S"):
    success, ini_date, end_date, msg = check_range_yyyy_mm_dd_hh_mm_ss(ini_date, end_date)
    if not success:
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    v_report = SRNodeDetailsBase(tipo=tipo, nombre=nombre, fecha_inicio=ini_date, fecha_final=end_date)
    if isTemporal(ini_date, end_date):
        report = SRNodeDetailsTemporal.objects(id_report=v_report.id_report).first()
    else:
        report = SRNodeDetailsPermanente.objects(id_report=v_report.id_report).first()
    if report is None:
        return dict(success=False,
                    msg="No existe el cálculo para este nodo en la fecha indicada"), status.HTTP_404_NOT_FOUND
    else:
        report.delete()
        success, final_report, msg = run_summary(ini_date, end_date, save_in_db=True, force=True)
        if not success:
            return dict(success=False, msg="No se puede calcular el reporte final"), status.HTTP_404_NOT_FOUND
        return dict(success=True, report=final_report.to_dict(),
                    msg=f"El reporte del nodo {nombre} ha sido eliminado, "
                        f"y el reporte final ha sido re-calculado"), status.HTTP_200_OK


def get_obtiene_reporte_disponibilidad_por_id_reporte(id_report="Id del reporte de detalle"):
    report = get_node_details_report_by_id_node(id_report)
    if report is None:
        return dict(success=False, report=None, msg="El reporte no ha sido encontrado"), status.HTTP_404_NOT_FOUND
    return dict(success=True, report=report.to_dict(), msg="Reporte encontrado"), status.HTTP_200_OK


def get_obtiene_detalle_reporte_disponibilidad(id_report="Id del reporte de detalle"):
    general_report = SRFinalReportPermanente.objects(id_report=id_report).first()
    if general_report is None:
        general_report = SRFinalReportTemporal.objects(id_report=id_report).first()
    if general_report is None:
        return dict(success=False, report=None, msg="El reporte no ha sido encontrado"), status.HTTP_404_NOT_FOUND
    detail_report_list = list()
    for node_report in general_report.reportes_nodos:
        if isTemporal(general_report.fecha_inicio, general_report.fecha_final):
            detail_report = SRNodeDetailsTemporal.objects(id_report=node_report.id_report).first()
        else:
            detail_report = SRNodeDetailsPermanente.objects(id_report=node_report.id_report).first()
        if detail_report is None:
            continue
        detail_report_list.append(detail_report.to_dict())
    dict_to_send = general_report.to_dict()
    dict_to_send["reportes_nodos_detalles"] = detail_report_list
    return dict(success=True, report=dict_to_send, msg="El reporte ha sido obtenido de manera correcta")

def get_obtiene_estado_by_id_report(id_report:str):
    query = TemporalProcessingStateReport.objects(id_report=id_report)
    if query.count() == 0:
        return dict(success=False, report=None, msg="El reporte no ha sido encontrado"), status.HTTP_404_NOT_FOUND
    report = query.first()
    return dict(success=True, report=report.to_dict(), msg="Reporte encontrado"), status.HTTP_200_OK


def get_obtiene_estado_calculo_reporte(ini_date: str = "yyyy-mm-dd H:M:S", end_date: str = "yyyy-mm-dd H:M:S", version: str = None):
    success, ini_date, end_date, msg = check_range_yyyy_mm_dd_hh_mm_ss(ini_date, end_date)
    if not success:
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    if version is None:
        query = SRNode.objects(document="SRNode")
    else:
        query = V2SRNode.objects(document=version)
    if query.count() == 0:
        return dict(success=False, msg="No se encuentran nodos que procesar"), status.HTTP_404_NOT_FOUND
    all_nodes = [n for n in query if n.activado]
    permanent_report = not isTemporal(ini_date, end_date)

    # scan reports within this date range:
    to_send = list()
    for node in all_nodes:
        report_id = get_report_id(node.tipo, node.nombre, ini_date, end_date)
        status_report = get_temporal_status(report_id)
        if status_report is None:
            continue
        to_send.append(status_report.to_summary())
    if len(to_send) == 0:
        return dict(success=False, msg="No se encuentran reportes para esta fecha"), status.HTTP_404_NOT_FOUND
    return dict(success=True, status=to_send), status.HTTP_200_OK

def get_details_report_by_id_report(id_report:str):
    node_report = get_node_details_report_by_id_node(id_report)
    if node_report is None:
        return dict(success=False, report=None, msg="El reporte no ha sido encontrado"), status.HTTP_404_NOT_FOUND
    return dict(success=True, report=node_report.to_dict(), msg="Reporte encontrado"), status.HTTP_200_OK
