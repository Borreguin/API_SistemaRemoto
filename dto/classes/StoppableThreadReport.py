"""
    This script allows to start and stop a thread.
"""
import threading
import time
import datetime as dt
import traceback

from settings import initial_settings as init
import pandas as pd
import requests

host = "localhost"
url = f"http://{host}:{init.PORT}/api/disp-sRemoto/disponibilidad/ini_date/end_date"
log = init.LogDefaultConfig("StoppableThreadReport.log").logger


class StoppableThreadReport(threading.Thread):
    def __init__(self, device, name, *args, **values):
        super().__init__(*args, **values)
        self.name = name
        self.device = device
        self.start_time = dt.datetime.now()
        self._stop = threading.Event()

    def run(self):
        to_print_time = dt.datetime.now()
        while not self._stop.is_set():
            self.start_time = dt.datetime.now()
            ts = dt.datetime.now().strftime('[%Y-%b-%d %H:%M:%S.%f]')
            success, values = self.device.scan()
            # if success:
            #     tags = self.device.tags()
            #     success, msg = write_in_db(values, dt.datetime.utcnow(), tags, self.device.device_type)
            #     if not success:
            #         self.device.log.warning(f"{ts} [{self.device.name}] \t{msg}")
            # else:
            #     self.device.log.warning(f"{ts} [{self.device.name}] \t{values}")

            # log the status of the device:
            delta = dt.datetime.now() - to_print_time
            if delta.total_seconds() > 4:
                to_print_time = dt.datetime.now()
                self.device.log.info(f"{ts} healthy: {round(self.device.healthy, 3)}, "
                                     f"avg_time: {round(self.device.avg_processing_seconds, 4)}, "
                                     f"registros: {len(values)} [{self.device.name}]")

            # sleep function does sleep in seconds
            scan_left_seconds = self.get_left_time_seconds()
            time.sleep(scan_left_seconds)

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def get_left_time_seconds(self):
        left_time = dt.datetime.now() - self.start_time
        remain_time = self.device.scan_ms / 1000 - left_time.total_seconds()
        return remain_time if remain_time > 0 else 0


def get_thread_by_name(name):
    threads = threading.enumerate()
    for thread in threads:
        if thread.getName() == name:
            return thread
    return None


def get_today():
    today = dt.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    return today


def get_period(today=None):
    if today is None:
        today = get_today()
    if today.day > 1:
        return today - dt.timedelta(days=today.day - 1), today
    else:
        yesterday = today - dt.timedelta(days=1)
        return yesterday - dt.timedelta(days=yesterday.day - 1), today


class ReportGenerator:
    def __init__(self, span: dt.timedelta, trigger: dt.timedelta, trigger_mail: dt.timedelta, *args, **values):
        super().__init__(*args, **values)
        self.span = span
        self.trigger = trigger
        self.trigger_mail = trigger_mail
        self.today = get_today()
        self.ini_date, self.end_date = get_period(self.today)
        self.ini_date, self.end_date = self.ini_date + self.trigger, self.end_date + self.trigger
        self.send_mail = self.today + self.trigger_mail
        self.date_range = pd.date_range(start=self.ini_date, end=self.end_date, freq=self.span)

    def __str__(self):
        return f"{self.ini_date}@{self.end_date} span:{self.span}"

    def update_time_parameters(self, today=None):
        if today is None:
            self.today = get_today()
        self.today = today
        self.ini_date, self.end_date = get_period(self.today)
        self.ini_date, self.end_date = self.ini_date + self.trigger, self.end_date + self.trigger
        self.send_mail = self.today + self.trigger_mail
        self.date_range = pd.date_range(start=self.ini_date, end=self.end_date, freq=self.span)

    def get_reports(self):
        reports = list()
        failed_reports = list()
        for ini, end in zip(self.date_range, self.date_range[1:]):
            url_to_send = url.replace("ini_date", ini.strftime('%Y-%m-%d %H:%M:%S'))
            url_to_send = url_to_send.replace("end_date", end.strftime('%Y-%m-%d %H:%M:%S'))
            try:
                response = requests.get(url_to_send)
                if response.status_code == 404:
                    # timeout 10 minutes max
                    response = requests.put(url_to_send, timeout=600)
                elif response.status_code != 200:
                    log.warning(response.content)
                json_data = response.json()
                if "report" in json_data.keys():
                    reports.append(json_data["report"])
            except Exception as e:
                msg = f"[{dt.datetime.now()}] No se puede conectar con la API. \n{str(e)}"
                details = f"{msg} \n{traceback.format_exc()}"
                log.error(details)
                failed_reports.append(url_to_send)
                continue
        is_ok = len(reports) == len(self.date_range[1:])
        msg = "Se han recuperado todos los reportes" if is_ok \
            else f"No fue posible recuperar los siguientes reportes: \n{failed_reports}"
        return is_ok, reports, msg

    def get_details(self, general_report:dict):
        print(general_report)


def test():
    span = dt.timedelta(days=1)
    trigger = dt.timedelta(hours=0)
    trigger_mail = dt.timedelta(hours=7)
    # today = dt.datetime(year=2021, month=2, day=1)
    today = get_today()
    report_generator = ReportGenerator(span, trigger, trigger_mail)
    report_generator.update_time_parameters(today=today)
    log.info(f"Empezando recopilación de reportes: {report_generator}")
    success, general_reports, msg = report_generator.get_reports()
    details_report = report_generator.get_details(general_reports[-1])
    log.info(msg)

    print(success, msg)


if __name__ == "__main__":
    if init.DEBUG:
        test()
