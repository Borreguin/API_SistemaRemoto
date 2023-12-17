from __future__ import annotations
import datetime as dt
from app.utils.utils import get_id


def get_report_id(tipo:str, nombre:str, fecha_inicio:dt.datetime|str, fecha_final:dt.datetime|str) -> str:
    inicio = fecha_inicio.strftime('%d-%m-%Y %H:%M') if isinstance(fecha_inicio, dt.datetime) else fecha_inicio
    fin = fecha_final.strftime('%d-%m-%Y %H:%M') if isinstance(fecha_final, dt.datetime) else fecha_final
    return get_id([nombre, tipo, inicio, fin])