"""
    This script allows to start and stop a thread.
"""
import threading
import os
import codecs
import datetime as dt
import traceback
from mongoengine import connect

from dto.classes.utils import get_today, get_thread_by_name
from dto.mongo_engine_handler.ProcessingState import TemporalProcessingStateReport
from dto.mongo_engine_handler.SRFinalReport.sRFinalReportBase import lb_unidad_negocio, lb_empresa, \
    lb_utr_id, lb_utr, lb_disponibilidad_promedio_utr, lb_protocolo
from settings import initial_settings as init
import pandas as pd
import requests
from my_lib.utils import get_last_day, get_dates_by_default, get_block
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
sns.set()

host = "localhost"
url_tags_report = f"http://{host}:{init.PORT}{init.API_PREFIX}/sRemoto/indisponibilidad/tags/json/ini_date/end_date"
url_disponibilidad_diaria = f"http://{host}:{init.PORT}{init.API_PREFIX}/sRemoto/disponibilidad/diaria/json/ini_date/end_date"
log = init.LogDefaultConfig("StoppableThreadMailReport.log").logger
lb_indisponible_minutes = "indisponible_minutos"
lb_disponibilidad = "disponibilidad"
lb_n_tags = "numero_tags"
lb_fecha_inicio = "fecha_inicio"
lb_fecha_final = "fecha_final"
lb_disponibilidad_promedio_porcentage = "disponibilidad_promedio_porcentage"
lb_p_tags = "porcentage_tags"
k_disp_tag_umbral = "disp_tag_umbral"
k_disp_utr_umbral = "disp_utr_umbral"
image_name = "disponibilidad.jpeg"
template_file = "reporte_diario_acumulado.html"

class StoppableThreadMailReport(threading.Thread):
    def __init__(self, name: str, mail_config: dict, parameters: dict, trigger: dt.timedelta = None,
                 ini_date: dt.datetime = None, end_date: dt.datetime = None, *args, **values):
        super().__init__(*args, **values)
        if trigger is None:
            trigger = dt.timedelta(hours=0)
        self.name = name
        self.mail_config = mail_config
        self.parameters = parameters
        self.trigger = trigger
        self.today = get_today()
        self.start_time = dt.datetime.now()
        self._stop = threading.Event()
        self.trigger_event = self.today + self.trigger if dt.datetime.now() < self.today + self.trigger else \
            self.today + dt.timedelta(days=1) + self.trigger
        self.seconds_to_sleep = 10
        if ini_date is None or end_date is None:
            ini_date, end_date = get_dates_by_default()
        self.ini_date = ini_date
        self.end_date = end_date

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
            # if dt.datetime.now() >= self.trigger_event:
            gen = ReportGenerator(url_daily_report=url_disponibilidad_diaria, url_tags_report=url_tags_report,
                                  parameters=self.parameters, ini_date=self.ini_date, end_date=self.end_date)
            gen.process_information()


class ReportGenerator:
    def __init__(self, url_daily_report: str, url_tags_report: str, parameters: dict, ini_date: dt.datetime = None,
                 end_date: dt.datetime = None, *args, **values):
        super().__init__(*args, **values)
        self.url_tags_report = url_tags_report
        self.url_daily_report = url_daily_report
        if ini_date is None or end_date is None:
            ini_date, end_date = get_dates_by_default()
        self.ini_date = ini_date
        self.end_date = end_date
        self.last_day = end_date - dt.timedelta(days=1)
        self.daily_summary = None
        self.daily_details = None
        self.tags_report = None
        self.to_report_tags = None
        self.to_report_details = None
        self.parameters = parameters

    def __str__(self):
        return f"{self.ini_date}@{self.end_date}"

    def get_daily_reports(self):
        url_to_send = url_disponibilidad_diaria.replace("ini_date", self.ini_date.strftime('%Y-%m-%d'))
        url_to_send = url_to_send.replace("end_date", self.end_date.strftime('%Y-%m-%d'))
        try:
            log.info(f"Ejecutando {url_to_send}")
            response = requests.get(url_to_send)
            if response.status_code == 200:
                log.info(f"Se ha ejecutado de manera correcta: {response}")
                report = response.json()["report"]
                self.daily_summary = pd.DataFrame(report["Resumen"])
                self.daily_details = pd.DataFrame(report["Detalles"])
            else:
                log.warning(f"No se ha ejecutado de manera correcta: \n{response.json()}")
        except Exception as e:
            log.error(f"Problema al ejecutar: {url_disponibilidad_diaria}: \n{str(e)}")
            self.daily_summary = None
            self.daily_details = None

    def get_tags_report(self):
        yesterday = self.end_date - dt.timedelta(days=1)
        url_to_send = url_tags_report.replace("ini_date", yesterday.strftime('%Y-%m-%d'))
        url_to_send = url_to_send.replace("end_date", self.end_date.strftime('%Y-%m-%d'))
        try:
            log.info(f"Ejecutando {url_to_send}")
            response = requests.get(url_to_send)
            if response.status_code == 200:
                log.info(f"Se ha ejecutado de manera correcta: {response}")
                json_data = response.json()
                self.tags_report = pd.DataFrame(json_data["reporte"])
            else:
                log.warning(f"El proceso no fue ejecutado correctamente: {response.json()}")
                self.tags_report = None

        except Exception as e:
            log.error(f"No se pudo realizar la consulta: {url_to_send} \n{str(e)}")
            self.tags_report = None

    def process_information(self):
        success, msg = self.process_tag_report()
        success, msg = self.process_daily_report()
        self.generate_html_report()
        print("hello")



    def process_tag_report(self):
        try:
            self.get_tags_report()
            # si los reportes no han sido procesados y no existen:
            if self.tags_report is None or self.tags_report.empty:
                return False, f"El reporte no puede ser generado debido a que no existe el reporte de tags"
            evaluation_minutes = (self.end_date - self.last_day).total_seconds() / 60
            disponibilidad = [(1-indis/evaluation_minutes)*100 for indis in self.tags_report[lb_indisponible_minutes]]
            self.tags_report[lb_disponibilidad] = disponibilidad
            df_result = pd.DataFrame(columns=[lb_empresa, lb_unidad_negocio, lb_utr_id, lb_utr, lb_n_tags, lb_p_tags])
            # Contar las tags que no cumplen con el umbral
            for idx, df in self.tags_report.groupby(by="UTR"):
                umbral = self.parameters[k_disp_tag_umbral]
                mask = df[lb_disponibilidad] < umbral
                df_low = df[mask]
                if df_low.empty:
                    continue
                percentage_tags = round(len(df_low.index) / len(df.index)*100, 2)
                row = {lb_empresa: df[lb_empresa].iloc[0], lb_unidad_negocio: df[lb_unidad_negocio].iloc[0],
                       lb_utr: df[lb_utr].iloc[0], lb_utr_id: df[lb_utr_id].iloc[0], lb_n_tags: len(df_low.index),
                       lb_p_tags: percentage_tags}
                df_result = df_result.append(row, ignore_index=True)
            self.to_report_tags = df_result
            return True, "Informe tags procesado de manera correcta"
        except Exception as e:
            msg = "Problema al ejecutar el reporte de tags"
            tb = traceback.format_exc()
            log.error(f"{msg}: \n{str(e)}\n{tb}")
            return False, msg

    def process_daily_report(self):
        try:
            self.get_daily_reports()
            if self.daily_summary is None or self.daily_summary.empty:
                return False, f"El reporte no puede ser generado debido a que no existe los reportes diarios"
            df_group_mean = self.daily_details.groupby(by=[lb_unidad_negocio, lb_utr, lb_protocolo]).mean()
            df_group_accu = self.daily_details.groupby(by=[lb_unidad_negocio, lb_utr, lb_protocolo]).sum()
            mask = df_group_mean[lb_disponibilidad_promedio_utr] < self.parameters[k_disp_utr_umbral]
            self.to_report_details = df_group_mean[mask]
            n = len(self.daily_summary.index)
            width = 0.25 * n if n > 15 else 1.8 * n
            fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(width, 2))
            x = list(self.daily_summary[lb_fecha_inicio]) + [self.daily_summary[lb_fecha_final].iloc[-1]]
            y = list(self.daily_summary[lb_disponibilidad_promedio_porcentage]) + \
                [self.daily_summary[lb_disponibilidad_promedio_porcentage].iloc[-1]]
            x = [dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S") for s in x]
            sns.lineplot(x=x, y=y, ax=ax, drawstyle='steps-post', palette="Blues")
            n_ticks = 10
            locator = mdates.AutoDateLocator(minticks=1, maxticks=n_ticks)
            if n <= n_ticks:
                ax.xaxis.set_major_locator(locator)
            else:
                ax.xaxis.set_minor_locator(locator)
            day_fmt = mdates.DateFormatter('%b')
            month_fmt = mdates.DateFormatter('%d')
            ax.xaxis.set_minor_formatter(day_fmt)
            ax.xaxis.set_major_formatter(month_fmt)
            fig.tight_layout()
            plot_file_path = os.path.join(init.IMAGES_REPO, image_name)
            fig.savefig(plot_file_path)
            return True, "Informes diarios procesados de manera correcta"
        except Exception as e:
            msg = "Problema al ejecutar los reportes diarios"
            tb = traceback.format_exc()
            log.error(f"{msg}: \n{str(e)}\n{tb}")
            return False, msg

    def generate_html_report(self):
        try:
            html_template_path = os.path.join(init.TEMPLATES_REPO, template_file)
            html_str = codecs.open(html_template_path, 'r', 'utf-8').read()
            utr_item_template = get_block("<!--INI: UTR_DISPONIBILIDAD-->", "<!--END: UTR_DISPONIBILIDAD-->", html_str)
            utr_table = str()
            for idx in self.to_report_details.index:
                u_negocio, utr, protocol = idx
                item = self.to_report_details.loc[idx]
                utr_item = utr_item_template.replace("#U_NEGOCIO", u_negocio)
                utr_item = utr_item.replace("#UTR", utr)
                utr_item = utr_item.replace("#PROTOCOLO", protocol)
                utr_item = utr_item.replace("#TIEMPO_ACUMULADO", item.loc[])

        except Exception as e:
            msg = "El reporte no ha podido ser generado"
            tb = traceback.format_exc()
            log.error(f"{msg}: \n{str(e)}\n{tb}")
            return False, msg

def test():
    mongo_config = init.MONGOCLIENT_SETTINGS
    connect(**mongo_config)
    rutine_name = "rutina_correo_electronico"
    trigger = dict(hours=23, minutes=00, seconds=0)
    mail_config = dict(from_mail="sistemaremoto@cenace.org.ec", users=["rsanchez@cenace.org.ec"],
                       admin=["rsanchez@cenace.org.ec"])
    parameters = dict(disp_utr_umbral=0.9, disp_tag_umbral=0.9)
    ini_date, end_date = get_dates_by_default()
    # ini_date, end_date = dt.datetime(year=2021, month=3, day=1), dt.datetime(year=2021, month=3, day=30)
    th_v = StoppableThreadMailReport(trigger=dt.timedelta(**trigger), name=rutine_name, mail_config=mail_config,
                                     parameters=parameters, ini_date=ini_date, end_date=end_date)
    th_v.save(msg="Configuración guardada")
    state = TemporalProcessingStateReport.objects(id_report=rutine_name).first()
    trigger = dt.timedelta(**state.info["trigger"])
    mail_config = state.info["mail_config"]
    th = get_thread_by_name(rutine_name)
    if th is None:
        th = StoppableThreadMailReport(name=rutine_name, trigger=trigger, mail_config=mail_config, parameters=parameters,
                                       ini_date=ini_date, end_date=end_date)
        th.start()
    else:
        th.stop()


if __name__ == "__main__":
    if init.DEBUG:
        test()
