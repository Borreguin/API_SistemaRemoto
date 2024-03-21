from __future__ import annotations

import datetime as dt
import traceback
from random import randint
from typing import Tuple, List

from app.common import report_node_log as log
from app.common.PI_connection.PIServer.PIServerBase import PIServerBase
from app.common.PI_connection.pi_connect import create_pi_server
from app.core.config import Settings
from app.core.v2CalculationEngine.NodeStatusCalculation import NodeStatusCalculation as nodeStatus
from app.db.db_util import get_node_details_report
from app.db.v1.ProcessingState import TemporalProcessingStateReport
from app.db.v2.entities.v2_sRConsignment import V2SRConsignment
from app.db.v2.entities.v2_sRNode import V2SRNode
from app.db.v2.v2SRNodeReport.V2SRNodeDetailsPermanent import V2SRNodeDetailsPermanent
from app.db.v2.v2SRNodeReport.V2SRNodeDetailsTemporal import V2SRNodeDetailsTemporal


def get_pi_server() -> PIServerBase:
    # seleccionando cualquiera disponible
    idx = randint(0, len(Settings.PISERVERS) - 1)
    log.info(f"PI aleatorio seleccionado: {Settings.PISERVERS[int(idx)]}")
    pi_server_name = Settings.PISERVERS[int(idx)]
    return create_pi_server(pi_server_name)

def create_v2sr_node_report(ini_report_date: dt.datetime, end_report_date: dt.datetime, node: V2SRNode, permanent: bool = False):
    if permanent:
        report_node = V2SRNodeDetailsPermanent(nodo=node, nombre=node.nombre, tipo=node.tipo,
                                               fecha_inicio=ini_report_date,
                                               fecha_final=end_report_date, id_node=node.id_node)
    else:
        report_node = V2SRNodeDetailsTemporal(nodo=node, nombre=node.nombre, tipo=node.tipo,
                                              fecha_inicio=ini_report_date,
                                              fecha_final=end_report_date, id_node=node.id_node)
    return report_node

def delete_v2sr_node_report_if_exists(report_id: str, permanent_report: bool) \
        -> Tuple[bool, str]:
    """ Observar si existe el nodo en la base de datos """
    try:
        node_details_report_db = get_node_details_report(report_id, permanent_report)
        report_already_exists = (node_details_report_db is not None)
        """ Si se desea guardar y ya existe y no es sobreescritura, no se continúa """
        if report_already_exists:
            node_details_report_db.delete()
            return True, "Reporte eliminado de manera correcta para reescritura"
        return True, "No es necesario eliminar el reporte"

    except Exception as e:
        msg = "Problema de concistencia en la base de datos"
        tb = traceback.format_exc()
        log.error(f"{msg} {str(e)} \n{tb}")
        return False, msg


def verify_pi_server_connection(pi_svr: PIServerBase, status_node: TemporalProcessingStateReport):
    try:
        pi_svr.server.Connect()
        return True, nodeStatus.CONNECTED
    except Exception as e:
        msg = f"No es posible la conexión con el servidor [{pi_svr.server.Name}]"
        log.error(f"{msg} \n[{str(e)}] \n[{traceback.format_exc()}]")
        status_node.failed()
        status_node.msg = msg
        status_node.update_now()
        return False, nodeStatus.NO_PI_CONNECTION


def get_active_entities(node: V2SRNode):
    if node.entidades is None or len(node.entidades) == 0:
        return False, "No hay entidades a procesar en el nodo", None
    entities = [e for e in node.entidades if e.activado]
    if len(entities) == 0:
        return False, "No hay entidades activas a procesar en el nodo", None
    return True, f"{len(entities)} entidades", entities

def unique_consignments(consignments: List[V2SRConsignment], new_consignments: List[V2SRConsignment]):
    if new_consignments is None:
        return consignments
    for new_consignment in new_consignments:
        found = False
        for consignment in consignments:
            if consignment.id_consignacion == new_consignment.id_consignacion:
                found = True
        if not found:
            consignments.append(new_consignment)
    return consignments


