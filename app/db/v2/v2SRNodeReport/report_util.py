from __future__ import annotations
import datetime as dt
from app.db.constants import SR_REPORTE_SISTEMA_REMOTO, V2_SR_FINAL_REPORT_LABEL
from app.utils.utils import get_id


def get_report_id(tipo:str, nombre:str, fecha_inicio:dt.datetime|str, fecha_final:dt.datetime|str) -> str:
    inicio = fecha_inicio.strftime('%d-%m-%Y %H:%M') if isinstance(fecha_inicio, dt.datetime) else fecha_inicio
    fin = fecha_final.strftime('%d-%m-%Y %H:%M') if isinstance(fecha_final, dt.datetime) else fecha_final
    return get_id([nombre, tipo, inicio, fin])

def get_final_report_id(tipo:str, fecha_inicio:dt.datetime|str, fecha_final:dt.datetime|str):
    inicio = fecha_inicio.strftime('%d-%m-%Y %H:%M') if isinstance(fecha_inicio, dt.datetime) else fecha_inicio
    fin = fecha_final.strftime('%d-%m-%Y %H:%M') if isinstance(fecha_final, dt.datetime) else fecha_final
    return get_id([tipo, inicio, fin])

def get_general_report_id(is_permanent:bool, report_ini_date:dt.datetime|str, report_end_date:dt.datetime|str):
    return get_report_id(SR_REPORTE_SISTEMA_REMOTO, f"{V2_SR_FINAL_REPORT_LABEL} - {is_permanent}", report_ini_date, report_end_date)