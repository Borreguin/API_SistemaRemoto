import time

from starlette import status

from app.core.repositories import local_repositories
from app.schemas.RequestSchemas import NodeRequest, NodesRequest
from flask_app.motor.master_scripts.eng_sRmaster import *
from app.utils.utils import check_date_yyyy_mm_dd_hh_mm_ss, is_active, save_in_file, isTemporal
import datetime as dt


def put_calcula_o_sobreescribe_disponibilidad_en_rango_fecha(ini_date, end_date):
    id = ini_date + "_" + end_date
    success1, ini_date = check_date_yyyy_mm_dd_hh_mm_ss(ini_date)
    success2, end_date = check_date_yyyy_mm_dd_hh_mm_ss(end_date)
    if not success1 or not success2:
        msg = "No se puede convertir: " + (ini_date if not success1 else end_date)
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    # check if there is already a calculation:
    path_file = os.path.join(local_repositories.TEMPORAL, "api_sR_cal_disponibilidad.json")
    time_delta = dt.timedelta(minutes=20)
    # puede el cálculo estar activo más de 20 minutos?
    active = is_active(path_file, id, time_delta)
    if active:
        return dict(success=False, result=None,
                    msg=f"Ya existe un cálculo en proceso con las fechas: {ini_date} al {end_date}. "
                        f"Tiempo restante: {int(time_delta.total_seconds() / 60)} min "
                        f"{time_delta.total_seconds() % 60} seg"), status.HTTP_409_CONFLICT

    # preparandose para cálculo: (permite bloquear futuras peticiones si ya existe un cálculo al momento)
    dict_value = dict(fecha=dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), activo=True)
    save_in_file(path_file, id, dict_value)

    # realizando el cálculo por cada nodo:
    success, report, msg = run_nodes_and_summarize(ini_date, end_date, save_in_db=True, force=True)
    # desbloqueando la instancia:
    dict_value["activo"] = False
    save_in_file(path_file, id, dict_value)
    result = report.to_dict() if success else None
    return dict(success=success, report=result, msg=msg), status.HTTP_200_OK if success else 409


def post_calcula_disponibilidad_en_rango_fecha(ini_date, end_date):
    success1, ini_date = check_date_yyyy_mm_dd_hh_mm_ss(ini_date)
    success2, end_date = check_date_yyyy_mm_dd_hh_mm_ss(end_date)
    if not success1 or not success2:
        msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    success1, result, msg1 = run_all_nodes(ini_date, end_date, save_in_db=True)
    not_calculated = [True for k in result.keys() if "No ha sido calculado" in result[k]]
    if all(not_calculated) and len(not_calculated) > 0:
        return dict(success=False, msg=dict(msg="No ha sido calculado completamente, "
                                                "ya existe algunos reportes en base de datos. "
                                                "Considere re-escribir el cálculo", detalle=msg1))
    if success1:
        success2, report, msg2 = run_summary(ini_date, end_date, save_in_db=True, force=True,
                                             results=result, log_msg=msg1)
        if success2:
            return dict(result=result, msg=msg1, report=report.to_dict()), status.HTTP_200_OK
        else:
            return dict(success=False, msg=msg2), status.HTTP_400_BAD_REQUEST
    elif not success1:
        return dict(success=False, msg=result), status.HTTP_400_BAD_REQUEST


def get_obtiene_disponibilidad_en_rango_fecha(ini_date: str = "yyyy-mm-dd H:M:S", end_date: str = "yyyy-mm-dd H:M:S"):
    success1, ini_date = check_date_yyyy_mm_dd_hh_mm_ss(ini_date)
    success2, end_date = check_date_yyyy_mm_dd_hh_mm_ss(end_date)
    if not success1 or not success2:
        msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    if isTemporal(ini_date, end_date):
        final_report_v = SRFinalReportTemporal(fecha_inicio=ini_date, fecha_final=end_date)
        final_report = SRFinalReportTemporal.objects(id_report=final_report_v.id_report).first()
    else:
        final_report_v = SRFinalReportPermanente(fecha_inicio=ini_date, fecha_final=end_date)
        final_report = SRFinalReportPermanente.objects(id_report=final_report_v.id_report).first()
    if final_report is None:
        return dict(success=False, msg="No existe reporte asociado"), status.HTTP_404_NOT_FOUND
    return dict(success=True, report=final_report.to_dict()), status.HTTP_200_OK


def put_calcula_o_sobrescribe_disponibilidad_nodo_en_rango_fecha(ini_date: str = "yyyy-mm-dd H:M:S",
                                                                 end_date: str = "yyyy-mm-dd H:M:S",
                                                                 request_data: NodeRequest = NodeRequest()):
    success1, ini_date = check_date_yyyy_mm_dd_hh_mm_ss(ini_date)
    success2, end_date = check_date_yyyy_mm_dd_hh_mm_ss(end_date)
    if not success1 or not success2:
        msg = "No se puede convertir: " + (ini_date if not success1 else end_date)
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
    success1, ini_date = check_date_yyyy_mm_dd_hh_mm_ss(ini_date)
    success2, end_date = check_date_yyyy_mm_dd_hh_mm_ss(end_date)
    if not success1 or not success2:
        msg = "No se puede convertir: " + (ini_date if not success1 else end_date)
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST

    success1, result, msg1 = run_node_list(request_data.nodos, ini_date, end_date, save_in_db=True, force=True)
    if success1:
        success2, report, msg2 = run_summary(ini_date, end_date, save_in_db=True, force=True,
                                             results=result, log_msg=msg1)
        if success2:
            return dict(result=result, msg=msg1, report=report.to_dict()), status.HTTP_200_OK
        else:
            return dict(success=False, msg=msg2), status.HTTP_409_CONFLICT
    elif not success1:
        return dict(success=False, msg=result), status.HTTP_409_CONFLICT


def post_calcula_disponibilidad_nodos_en_lista(ini_date: str = "yyyy-mm-dd H:M:S", end_date: str = "yyyy-mm-dd H:M:S",
                                               request_data: NodesRequest = NodesRequest()):
    success1, ini_date = check_date_yyyy_mm_dd_hh_mm_ss(ini_date)
    success2, end_date = check_date_yyyy_mm_dd_hh_mm_ss(end_date)
    if not success1 or not success2:
        msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
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
    success1, ini_date = check_date_yyyy_mm_dd_hh_mm_ss(ini_date)
    success2, end_date = check_date_yyyy_mm_dd_hh_mm_ss(end_date)
    if not success1 or not success2:
        msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    not_found = list()
    deleted = list()
    for node in request_data.nodos:
        sr_node = SRNode.objects(nombre=node).first()
        if sr_node is None:
            not_found.append(node)
            continue
        report_v = SRNodeDetailsBase(nodo=sr_node, nombre=sr_node.nombre, tipo=sr_node.tipo,
                                     fecha_inicio=ini_date, fecha_final=end_date)
        # delete status report if exists
        status_report = TemporalProcessingStateReport.objects(id_report=report_v.id_report).first()
        if status_report is not None:
            status_report.delete()
        if isTemporal(ini_date, end_date):
            report = SRNodeDetailsTemporal.objects(id_report=report_v.id_report).first()
        else:
            report_v = SRNodeDetailsPermanente(nodo=sr_node, nombre=sr_node.nombre, tipo=sr_node.tipo,
                                               fecha_inicio=ini_date, fecha_final=end_date)
            report = SRNodeDetailsPermanente.objects(id_report=report_v.id_report).first()
        if report is None:
            not_found.append(node)
            continue
        report.delete()
        deleted.append(node)

    if len(deleted) == 0:
        return dict(success=False, deleted=deleted, not_found=not_found), status.HTTP_404_NOT_FOUND
    success, _, msg = run_summary(ini_date, end_date, save_in_db=True, force=True)
    if not success:
        return dict(success=False, deleted=[], not_found=not_found), status.HTTP_400_BAD_REQUEST
    return dict(success=True, deleted=deleted, not_found=not_found), status.HTTP_200_OK


def get_obtiene_disponibilidad_por_tipo_nodo(tipo="tipo de nodo", nombre="nombre del nodo",
                                             ini_date: str = "yyyy-mm-dd H:M:S",
                                             end_date: str = "yyyy-mm-dd H:M:S"):
    success1, ini_date = check_date_yyyy_mm_dd_hh_mm_ss(ini_date)
    success2, end_date = check_date_yyyy_mm_dd_hh_mm_ss(end_date)
    if not success1 or not success2:
        msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
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
        return report.to_dict(), status.HTTP_200_OK


def delete_elimina_reporte_disponibilidad_de_nodo(tipo="tipo de nodo", nombre="nombre del nodo",
                                                  ini_date: str = "yyyy-mm-dd H:M:S",
                                                  end_date: str = "yyyy-mm-dd H:M:S"):
    success1, ini_date = check_date_yyyy_mm_dd_hh_mm_ss(ini_date)
    success2, end_date = check_date_yyyy_mm_dd_hh_mm_ss(end_date)
    if not success1 or not success2:
        msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
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
    report = SRNodeDetailsPermanente.objects(id_report=id_report).first()
    if report is None:
        report = SRNodeDetailsTemporal.objects(id_report=id_report).first()
    if report is None:
        return dict(success=False, report=None, msg="El reporte no ha sido encontrado"), status.HTTP_404_NOT_FOUND
    return dict(success=True, report=report.to_dict(), msg="Reporte encontrado")


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


def get_obtiene_estado_calculo_reporte(ini_date: str = "yyyy-mm-dd H:M:S", end_date: str = "yyyy-mm-dd H:M:S"):
    success1, ini_date = u.check_date_yyyy_mm_dd_hh_mm_ss(ini_date)
    success2, end_date = u.check_date_yyyy_mm_dd_hh_mm_ss(end_date)
    if not success1 or not success2:
        msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    # check the existing nodes:
    all_nodes = SRNode.objects(document="SRNode")
    all_nodes = [n for n in all_nodes if n.activado]
    if len(all_nodes) == 0:
        msg = f"No se encuentran nodos que procesar"
        return dict(success=False, msg=msg), status.HTTP_404_NOT_FOUND

    # scan reports within this date range:
    to_send = list()
    for node in all_nodes:
        if isTemporal(ini_date, end_date):
            v_report = SRNodeDetailsTemporal(nodo=node, nombre=node.nombre, tipo=node.tipo,
                                             fecha_inicio=ini_date, fecha_final=end_date)
        else:
            v_report = SRNodeDetailsPermanente(nodo=node, nombre=node.nombre, tipo=node.tipo,
                                               fecha_inicio=ini_date, fecha_final=end_date)
        tmp_report = TemporalProcessingStateReport.objects(id_report=v_report.id_report).first()

        if tmp_report is None:
            for i in range(20):
                tmp_report = TemporalProcessingStateReport.objects(id_report=v_report.id_report).first()
                if tmp_report is not None:
                    break
                else:
                    time.sleep(2)
        to_send.append(tmp_report.to_summary())
    if len(all_nodes) == len(to_send):
        return dict(success=True, status=to_send), status.HTTP_200_OK
    else:
        return dict(success=False, status=None), status.HTTP_409_CONFLICT
