"""
    This script allows to start and stop a thread.
"""
import threading
import time
import datetime as dt
import traceback

from dto.classes.utils import get_today, get_period, get_thread_by_name
from settings import initial_settings as init
import pandas as pd
import requests

host = "localhost"
url_disponibilidad_diaria = f"http://{host}:{init.PORT}{init.API_PREFIX}/admin-report/run/reporte/diario"
log = init.LogDefaultConfig("StoppableThreadDailyReport.log").logger


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
        self.executed = False

    def update(self):
        self.today = get_today()
        self.start_time = dt.datetime.now()
        self.trigger_event = self.today + self.trigger if dt.datetime.now() < self.today + self.trigger else \
            self.today + dt.timedelta(days=1) + self.trigger

    def run(self):
        while not self._stop.is_set():
            if dt.datetime.now() >= self.trigger_event and not self.executed:
                response = requests.put(url_disponibilidad_diaria)
                log.info(response.json())
                self.update()
                self.executed = False
            scan_left_seconds = self.get_left_time_seconds()
            log.info("")
            time.sleep(scan_left_seconds)

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def get_left_time_seconds(self):
        left_time = self.trigger_event - dt.datetime.now()
        remain_time = (left_time.total_seconds()/2)
        return remain_time if remain_time > 0 else 0


def test():
    trigger = dt.timedelta(hours=19, minutes=41)
    rutine_name = "rutina_diaria"
    th = get_thread_by_name(rutine_name)
    if th is None:
        th = StoppableThreadDailyReport(trigger=trigger, name=rutine_name)
        th.start()
    else:
        th.stop()


if __name__ == "__main__":
    if init.DEBUG:
        test()
