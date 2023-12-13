
import datetime as dt
from typing import List

from app.common import error_log
from app.common.PI_connection.PIServer.PIServerBase import PIServerBase
from app.core.v2CalculationEngine.DatetimeRange import DateTimeRange
from app.core.v2CalculationEngine.constants import yyyy_mm_dd_hh_mm_ss
from app.core.v2CalculationEngine.node.node_util import processing_unavailability_of_tags
from app.db.db_util import get_consignments
from app.db.v2.entities.v2_sRConsignment import V2SRConsignment
from app.db.v2.entities.v2_sREntity import V2SREntity
from app.db.v2.entities.v2_sRInstallation import V2SRInstallation


def head(ini: dt.datetime, end: dt.datetime):
    return f"[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] ({ini}@{end})"

def generate_time_ranges(consignaciones: List[V2SRConsignment], ini_date: dt.datetime, end_date: dt.datetime):
    # La función encuentra el periodo en el cual se puede examinar la disponibilidad:
    if len(consignaciones) == 0:
        return [DateTimeRange(ini_date, end_date)]

    # caso inicial:
    # * ++ tiempo de análisis ++*
    # [ periodo de consignación ]
    time_ranges = list()  # lleva la lista de periodos válidos
    tail = None  # inicialización
    end = end_date  # inicialización posible fecha final valida
    if consignaciones[0].fecha_inicio < ini_date:
        # [-----*++++]+++++++++++++++++++++*------
        tail = consignaciones[0].fecha_final  # lo que queda restante a analizar
        end = end_date  # por ser caso inicial se asume que se puede hacer el cálculo hasta el final del periodo

    elif consignaciones[0].fecha_inicio > ini_date and consignaciones[0].fecha_final < end_date:
        # --*++++[++++++]+++++++++*---------------
        start = ini_date  # fecha desde la que se empieza un periodo válido para calc. disponi
        # end = consignaciones[0].fecha_inicio  # fecha fin del periodo válido para calc. disponi
        tail = consignaciones[0].fecha_final  # siguiente probable periodo (lo que queda restante a analizar)
        time_ranges = [DateTimeRange(start, consignaciones[0].fecha_inicio)]  # primer periodo válido

    elif consignaciones[0].fecha_inicio > ini_date and consignaciones[0].fecha_final >= end_date:
        # --*++++[+++++++++++++++*-----]----------
        # este caso es definitivo y no requiere continuar más alla:
        start = ini_date  # fecha desde la que se empieza un periodo válido para calc. disponi
        end = consignaciones[0].fecha_inicio  # fecha fin del periodo válido para calc. disponi
        return [DateTimeRange(start, end)]

    elif consignaciones[0].fecha_inicio == ini_date and consignaciones[0].fecha_final < end_date:
        # --*[++++++++++]+++++++++*---------------
        start = consignaciones[0].fecha_final  # fecha desde la que se empieza un periodo válido para calc. disponi
        end = end_date  # fecha fin del periodo válido para calc. disponi
        tail = consignaciones[0].fecha_final  # siguiente probable periodo (lo que queda restante a analizar)
        time_ranges = []  # No hay periodo valido a evaluar

    elif consignaciones[0].fecha_inicio == ini_date and consignaciones[0].fecha_final == end_date:
        # ---*[+++++++]*---
        # este caso es definitivo y no requiere continuar más alla
        # nada que procesar en este caso
        return []

    # creando los demás rangos:
    for c in consignaciones[1:]:
        start = tail
        end = c.fecha_inicio
        if c.fecha_final < end_date:
            time_ranges.append(DateTimeRange(start, end))
            tail = c.fecha_final
        else:
            end = c.fecha_inicio
            break
    # ultimo caso:
    time_ranges.append(DateTimeRange(tail, end))
    return time_ranges

def get_date_time_ranges_using_consignments(element_id:str, ini_date: dt.datetime, end_date: dt.datetime):
    consignments: List[V2SRConsignment] = get_consignments(element_id, ini_date, end_date)
    return generate_time_ranges(consignments, ini_date, end_date), consignments

def get_date_time_ranges_using_time_ranges(element_id:str, time_ranges:List[DateTimeRange]):
    result_range_list = list()
    consignments_list = list()
    for dt_range in time_ranges:
        time_range_list, consignment_list = get_date_time_ranges_using_consignments(element_id, dt_range.start, dt_range.end)
        result_range_list += time_range_list
        consignments_list += consignment_list
    return result_range_list, consignments_list

def process_entity(entity: V2SREntity, pi_svr: PIServerBase, ini_date: dt.datetime, end_date: dt.datetime):
    if entity.instalaciones is None or len(entity.instalaciones) == 0:
        return False, f"No hay instalaciones a procesar para esta entidad {entity}", None
    entity_time_ranges, entity_consignments = (
        get_date_time_ranges_using_consignments(entity.get_document_id(), ini_date, end_date)
    )
    installation_consignments = list()
    bahia_consignments = list()
    for installation in entity.instalaciones:
        try:
            instalacion: V2SRInstallation = installation.fetch()
            inst_time_ranges, inst_consignments = (
                get_date_time_ranges_using_time_ranges(instalacion.get_document_id(), entity_time_ranges)
            )
            installation_consignments +=  inst_consignments
            if instalacion.bahias is None or len(instalacion.bahias) == 0:
                return False, f"No hay bahias a procesar para esta instalación {instalacion}", None
            for bahia in instalacion.bahias:
                bahia_time_ranges, bahia_consignments_list = (
                    get_date_time_ranges_using_time_ranges(bahia.get_document_id(), inst_time_ranges)
                )
                bahia_consignments += bahia_consignments_list
                if bahia.tags is None or len(bahia.tags) == 0:
                    return False, f"No hay tags a procesar para bahia {bahia}", None
                success, msg, df_tag_unavailability = processing_unavailability_of_tags(bahia.tags, bahia_time_ranges, pi_svr)

        except Exception as e:
            error_log.error(f'Not able to process an installation: {str(e)}')