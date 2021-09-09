"""
    This script allows to start and stop a thread.
"""
import threading
import time
import datetime as dt
import traceback
from mongoengine import connect

import flask_app.settings.LogDeafultConfig
from flask_app.dto.classes.utils import get_today, get_thread_by_name
from flask_app.dto.mongo_engine_handler.ProcessingState import TemporalProcessingStateReport
from flask_app.settings import initial_settings as init
from flask_app.my_lib.utils import get_dates_by_default
import requests

host = "localhost"
url_disponibilidad_diaria = f"http://{host}:{init.API_PORT}{init.API_PREFIX}/admin-report/run/reporte/diario"
url_disponibilidad_mes = f"http://{host}:{init.API_PORT}{init.API_PREFIX}/disp-sRemoto/disponibilidad/ini_date/end_date"
log = flask_app.settings.LogDeafultConfig.LogDefaultConfig("StoppableThreadDailyReport.log").logger


class StoppableThreadDailyReport(threading.Thread):
    def __init__(self, name, trigger: dt.timedelta = None, *args, **values):
        super().__init__(*args, **values)
        if trigger is None:
            trigger = dt.timedelta(hours=0)
        self.name = name
        self.trigger = trigger
        self.today = get_today()
        self.start_time = dt.datetime.now()
        self._stop = threading.Event()
        self.trigger_event = self.today + self.trigger if dt.datetime.now() < self.today + self.trigger else \
            self.today + dt.timedelta(days=1) + self.trigger
        self.seconds_to_sleep = 10
        self.daemon = True

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

    def get_left_time_seconds(self):
        left_time = self.trigger_event - dt.datetime.now()
        remain_time = left_time.total_seconds() if left_time.total_seconds() < self.seconds_to_sleep \
            else self.seconds_to_sleep
        return remain_time if remain_time > 0 else 0

    def save(self, msg=None):
        info = dict(trigger=dict(seconds=self.trigger.seconds, days=self.trigger.days))
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
            try:
                self.update_from_db()
                if dt.datetime.now() >= self.trigger_event:
                    msg = f"Executing: {url_disponibilidad_diaria}"
                    log.info(msg)
                    daily_response = requests.put(url_disponibilidad_diaria)
                    msg = f"This process was executed: [{daily_response}]"
                    log.info(msg)
                    if dt.datetime.now().day == 1:
                        ini_date, end_date = get_dates_by_default()
                        url_to_send = url_disponibilidad_mes.replace("ini_date", ini_date.strftime("%Y-%m-%d"))
                        url_to_send = url_to_send.replace("end_date", end_date.strftime("%Y-%m-%d"))
                        msg = f"Executing: {url_to_send}"
                        month_response = requests.put(url_to_send)
                        log.info(msg)
                        msg = f"This process was executed: [{month_response}]"
                        log.info(msg)
                    msg = f"All was correctly executed"
                    msg = f"{msg} \nWainting until {self.trigger_event}"
                    self.save(msg)
                    self.update()
                    log.info(msg)
            except Exception as e:
                log.error(f"Error al procesar la información \n{str(e)}\n{traceback.format_exc()}")
            if n_iter % 50 == 0 or n_iter == 0:
                msg = f"The process is running. Waiting until {self.trigger_event}"
                log.info(msg)
                self.save(msg)
            n_iter = n_iter + 1 if n_iter <= 500 else 0
            left_time = self.get_left_time_seconds()
            time.sleep(left_time)


def test():
    mongo_config = init.MONGOCLIENT_SETTINGS
    connect(**mongo_config)
    rutine_name = "rutina_de_reporte_diario"
    trigger = dict(hours=7, minutes=29, seconds=0)
    th_v = StoppableThreadDailyReport(trigger=dt.timedelta(**trigger), name=rutine_name)
    th_v.save(msg="Configuración guardada")
    state = TemporalProcessingStateReport.objects(id_report=rutine_name).first()
    trigger = dt.timedelta(**state.info["trigger"])
    th = get_thread_by_name(rutine_name)
    if th is None:
        th = StoppableThreadDailyReport(trigger=trigger, name=rutine_name)
        th.start()
    else:
        th.stop()


if __name__ == "__main__":
    if init.DEBUG:
        test()
