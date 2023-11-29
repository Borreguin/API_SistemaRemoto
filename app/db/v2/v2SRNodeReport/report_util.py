import datetime as dt
from app.utils.utils import get_id


def get_report_id(tipo:str, nombre:str, fecha_inicio:dt.datetime, fecha_final:dt.datetime) -> str:
    return get_id([nombre, tipo, fecha_inicio.strftime('%d-%m-%Y %H:%M'), fecha_final.strftime('%d-%m-%Y %H:%M')])