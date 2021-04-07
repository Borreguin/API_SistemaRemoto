"""
    This script allows to start and stop a thread.
"""
import threading
import time
import datetime as dt
from mongoengine import connect

from dto.classes.utils import get_today
from dto.mongo_engine_handler.ProcessingState import TemporalProcessingStateReport
from settings import initial_settings as init
import pandas as pd
import requests

host = "localhost"
url_tags_report = f"http://{host}:{init.PORT}{init.API_PREFIX}/sRemoto/indisponibilidad/tags/json/ini_date/end_date"
url_disponibilidad_diaria = f"http://{host}:{init.PORT}{init.API_PREFIX}/sRemoto/disponibilidad/diaria/json"
log = init.LogDefaultConfig("StoppableThreadMailReport.log").logger


class StoppableThreadMailReport(threading.Thread):
    def __init__(self, name: str, mail_config: dict, trigger: dt.timedelta = None, *args, **values):
        super().__init__(*args, **values)
        if trigger is None:
            trigger = dt.timedelta(hours=0)
        self.name = name
        self.mail_config = mail_config
        self.trigger = trigger
        self.today = get_today()
        self.start_time = dt.datetime.now()
        self._stop = threading.Event()
        self.trigger_event = self.today + self.trigger if dt.datetime.now() < self.today + self.trigger else \
            self.today + dt.timedelta(days=1) + self.trigger
        self.seconds_to_sleep = 10

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def update(self):
        self.today = get_today()
        self.start_time = dt.datetime.now()
        self.trigger_event = self.today + self.trigger if dt.datetime.now() < self.today + self.trigger else \
            self.today + dt.timedelta(days=1) + self.trigger

    def update_from_db(self):
        state = TemporalProcessingStateReport.objects(id_report=self.name).first()
        if state is not None:
            self.trigger = dt.timedelta(**state.info["trigger"])
            self.mail_config = state.info["mail_config"]

    def get_left_time_seconds(self):
        left_time = self.trigger_event - dt.datetime.now()
        remain_time = left_time.total_seconds() if left_time.total_seconds() < self.seconds_to_sleep \
            else self.seconds_to_sleep
        return remain_time if remain_time > 0 else 0

    def save(self, msg=None):
        info = dict(trigger=dict(seconds=self.trigger.seconds, days=self.trigger.days), mail_config=self.mail_config)
        state = TemporalProcessingStateReport.objects(id_report=self.name).first()
        if state is None:
            state = TemporalProcessingStateReport(id_report=self.name, info=info, msg=msg)
            state.save()
        else:
            state.update(info=info, created=dt.datetime.now(), msg=msg)

    def run(self):
        n_iter = 0
        log.info("Starting this routine")
        while not self._stop.is_set():
            self.update_from_db()
            if dt.datetime.now() >= self.trigger_event:
                gen = ReportGenerator(url_disponibilidad_diaria, url_tags_report,)


class ReportGenerator:
    def __init__(self, url_daily_report: str, url_tags_report: str, ini_date: dt.datetime,
                 end_date: dt.datetime, *args, **values):
        super().__init__(*args, **values)
        self.url_tags_report = url_tags_report
        self.url_daily_report = url_daily_report
        self.ini_date = ini_date
        self.end_date = end_date
        self.daily_reports = None
        self.tags_report = None

    def __str__(self):
        return f"{self.ini_date}@{self.end_date}"

    def get_daily_reports(self):
        try:
            log.info(f"Ejecutando {url_disponibilidad_diaria}")
            response = requests.get(url_disponibilidad_diaria)
            if response.status_code == 200:
                log.info(f"Se ha ejecutado de manera correcta: {response}")
                self.daily_reports = response.json()["report"]
            else:
                log.warning(f"No se ha ejecutado de manera correcta: \n{response.json()}")
        except Exception as e:
            log.error(f"Problema al ejecutar: {url_disponibilidad_diaria}: \n{str(e)}")
            self.daily_reports = None

    def get_tags_report(self):
        url_to_send = url_tags_report.replace("ini_date", self.ini_date.strftime('%Y-%m-%d'))
        url_to_send = url_to_send.replace("end_date", self.end_date.strftime('%Y-%m-%d'))
        try:
            response = requests.get(url_to_send)
            if response.status_code != 200:
                log.warning(f"El proceso no fue ejecutado correctamente: {response.json()}")
                self.tags_report = None
            else:
                json_data = response.json()
                self.tags_report = pd.DataFrame(json_data["reporte"])
        except Exception as e:
            log.error(f"No se pudo realizar la consulta: {url_to_send} \n{str(e)}")
            self.tags_report = None

    def run(self):
        self.get_daily_reports()
        self.tags_report()
        if self.daily_reports is None or self.tags_report is None:
            missing = "el reporte diario" if self.daily_reports else "el reporte de tags"
            return False, f"El reporte no puede ser generado debido a que no existe {missing}"
        print(self.daily_reports, self.tags_report)


def test():
    mongo_config = init.MONGOCLIENT_SETTINGS
    connect(**mongo_config)
    rutine_name = "rutina_correo_electronico"
    trigger = dict(hours=23, minutes=00, seconds=0)
    mail_config = dict(from_mail="sistemaremoto@cenace.org.ec", to=["rsanchez@cenace.org.ec"])
    th_v = StoppableThreadMailReport(trigger=dt.timedelta(**trigger), name=rutine_name, mail_config=mail_config)
    th_v.save(msg="Configuración guardada")


if __name__ == "__main__":
    if init.DEBUG:
        test()
