from __future__ import annotations
import datetime as dt
from typing import List, Tuple

from app.core.v2CalculationEngine.DatetimeRange import DateTimeRange
from app.db.db_util import get_consignments
from app.db.v2.entities.v2_sRConsignment import V2SRConsignment


def head(ini: dt.datetime, end: dt.datetime):
    return f"({ini}@{end}) \n"

def generate_time_ranges(consignaciones: List[V2SRConsignment], ini_date: dt.datetime, end_date: dt.datetime) -> List[DateTimeRange]:
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

def get_date_time_ranges_from_consignment_time_ranges(element_id:str, time_ranges:List[DateTimeRange]) \
        -> Tuple[List[DateTimeRange], List[V2SRConsignment]]:
    result_range_list = list()
    consignments_list = list()
    for dt_range in time_ranges:
        time_range_list, consignment_list = get_date_time_ranges_using_consignments(element_id, dt_range.start, dt_range.end)
        result_range_list += time_range_list
        consignments_list += consignment_list
    return result_range_list, consignments_list