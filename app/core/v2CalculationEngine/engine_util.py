
import datetime as dt
from typing import List

from app.common.PI_connection.pi_util import create_time_range
from app.core.v2CalculationEngine.constants import yyyy_mm_dd_hh_mm_ss
from app.db.v1.Info.Consignment import Consignment



def head(ini: dt.datetime, end: dt.datetime):
    return f"[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] ({ini}@{end})"

def generate_time_ranges(consignaciones: List[Consignment], ini_date: dt.datetime, end_date: dt.datetime):
    # La función encuentra el periodo en el cual se puede examinar la disponibilidad:
    if len(consignaciones) == 0:
        return [create_time_range(ini_date, end_date)]

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
        time_ranges = [create_time_range(start, consignaciones[0].fecha_inicio)]  # primer periodo válido

    elif consignaciones[0].fecha_inicio > ini_date and consignaciones[0].fecha_final >= end_date:
        # --*++++[+++++++++++++++*-----]----------
        # este caso es definitivo y no requiere continuar más alla:
        start = ini_date  # fecha desde la que se empieza un periodo válido para calc. disponi
        end = consignaciones[0].fecha_inicio  # fecha fin del periodo válido para calc. disponi
        return [create_time_range(start, end)]

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
            time_ranges.append(create_time_range(start, end))
            tail = c.fecha_final
        else:
            end = c.fecha_inicio
            break
    # ultimo caso:
    time_ranges.append(create_time_range(tail, end))
    return time_ranges