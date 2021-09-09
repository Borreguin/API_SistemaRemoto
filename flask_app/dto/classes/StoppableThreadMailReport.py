"""
    This script allows to start and stop a thread.
"""
import re
import threading
import os
import codecs
import datetime as dt
import time
import traceback
from mongoengine import connect

import flask_app.settings.LogDeafultConfig
from flask_app.dto.classes.utils import get_today, get_thread_by_name
from flask_app.dto.mongo_engine_handler.ProcessingState import TemporalProcessingStateReport
from flask_app.dto.mongo_engine_handler.SRFinalReport.sRFinalReportBase import lb_unidad_negocio, lb_empresa, \
    lb_utr_id, lb_utr, lb_dispo_promedio_utr, lb_protocolo, lb_indisponible_minutos_promedio
from flask_app.my_lib.SendMail.send_mail import report_error, send_mail
from flask_app.settings import initial_settings as init
import pandas as pd
import requests
from flask_app.my_lib.utils import get_dates_by_default, get_block, replace_block, save_html
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
sns.set()

host = "10.30.250.52"
# URL TAGS report:
# url_tags_base = f"http://{host}:{init.API_PORT}{init.API_PREFIX}/sRemoto/indisponibilidad/tags"
url_tags_base = f"http://{host}{init.API_PREFIX}/sRemoto/indisponibilidad/tags"
url_tags_report = f"{url_tags_base}/json/ini_date/end_date"
url_tags_json = f"{url_tags_base}/json"
url_tags_excel = f"{url_tags_base}/excel"

# URL DISPONIBILIDAD report:
# url_disponibilidad_base = f"http://{host}:{init.API_PORT}{init.API_PREFIX}/sRemoto/disponibilidad/diaria"
url_disponibilidad_base = f"http://{host}{init.API_PREFIX}/sRemoto/disponibilidad/diaria"
url_disponibilidad_diaria = f"{url_disponibilidad_base}/json/ini_date/end_date"
url_disponibilidad_json = f"{url_disponibilidad_base}/json"
url_disponibilidad_excel = f"{url_disponibilidad_base}/excel"

# LOGS:
log = flask_app.settings.LogDeafultConfig.LogDefaultConfig("StoppableThreadMailReport.log").logger
lb_indisponible_minutes = "indisponible_minutos"
lb_disponibilidad = "disponibilidad"
lb_indisponible_acumulado_minutos = "indisponibilidad_acumulado_minutos"
lb_n_tags = "numero_tags"
lb_fecha_inicio = "fecha_inicio"
lb_fecha_final = "fecha_final"
lb_disponibilidad_promedio_porcentage = "disponibilidad_promedio_porcentage"
lb_p_tags = "porcentage_tags"
lb_total_tags = "total_tags"
k_disp_tag_umbral = "disp_tag_umbral"
k_disp_utr_umbral = "disp_utr_umbral"

# Templates:
image_name = "disponibilidad.jpeg"
template_file = "reporte_diario_acumulado.html"
html_path = os.path.join(init.REPORTS_REPO, "reporte_diario.html")
html_error_template_path = os.path.join(init.TEMPLATES_REPO, "reportar.html")
log_file = os.path.join(init.LOGS_REPO, "StoppableThreadMailReport.log")

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
        self.daemon = True

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def update(self):
        self.today = get_today()
        self.start_time = dt.datetime.now()
        self.ini_date, self.end_date = get_dates_by_default()
        self.trigger_event = self.today + self.trigger if dt.datetime.now() < self.today + self.trigger else \
            self.today + dt.timedelta(days=1) + self.trigger

    def update_from_db(self):
        state = TemporalProcessingStateReport.objects(id_report=self.name).first()
        if state is not None:
            self.trigger = dt.timedelta(**state.info["trigger"])
            self.mail_config = state.info["mail_config"]
            self.parameters = state.info["parameters"]

    def get_left_time_seconds(self):
        left_time = self.trigger_event - dt.datetime.now()
        remain_time = left_time.total_seconds() if left_time.total_seconds() < self.seconds_to_sleep \
            else self.seconds_to_sleep
        return remain_time if remain_time > 0 else 0

    def save(self, msg=None):
        info = dict(trigger=dict(seconds=self.trigger.seconds, days=self.trigger.days), mail_config=self.mail_config,
                    parameters=self.parameters)
        state = TemporalProcessingStateReport.objects(id_report=self.name).first()
        if state is None:
            state = TemporalProcessingStateReport(id_report=self.name, info=info, msg=msg)
            state.save()
        else:
            state.update(info=info, created=dt.datetime.now(), msg=msg)

    def send_report(self, subject):
        try:
            from_email = self.mail_config["from_email"]
            emails = self.mail_config["users"]
            html_str = codecs.open(html_path, 'r', 'utf-8').read()
            regex = 'src=(".*(\\.jpeg|\\.png)"){1}'
            image_list = re.findall(regex, html_str)
            image_list = [im[0].replace('"', '') for im in image_list]
            send_mail(html_str, subject, emails, from_email, image_list)
        except Exception as e:
            msg = "No se ha podido enviar el reporte"
            tb = traceback.format_exc()
            log.error(f"{msg}\n{str(e)}\n{tb}")

    def send_error(self, descripcion, detalle):
        try:
            from_email = self.mail_config["from_email"]
            emails = self.mail_config["admin"]
            report_error(descripcion, detalle, from_email, emails, log_file)
        except Exception as e:
            msg = "No se ha podido enviar el reporte de error"
            tb = traceback.format_exc()
            log.error(f"{msg}\n{str(e)}\n{tb}")

    def run(self):
        n_iter = 0
        log.info("Starting this routine")
        while not self._stop.is_set():
            try:
                self.update_from_db()
                if dt.datetime.now() >= self.trigger_event:
                    gen = ReportGenerator(url_daily_report=url_disponibilidad_diaria, url_tags_report=url_tags_report,
                                          parameters=self.parameters, ini_date=self.ini_date, end_date=self.end_date)

                    # generación del reporte diario para enviar vía mail
                    success, msg = gen.process_information()
                    log.info(msg)
                    # enviando email de reporte o error:
                    if success:
                        self.send_report("Reporte disponibilidad acumulada")
                    else:
                        self.send_error("reporte diario acumulado", msg)
                    self.save(msg)
                    self.update()
                    msg = f"{msg} Waiting until {self.trigger_event}"
                    log.info(msg)
            except Exception as e:
                log.error(f"Ha ocurrido un error al procesar la información \n{str(e)}\n{traceback.format_exc()}")
            # after each interaction:
            if n_iter % 50 == 0 or n_iter == 0:
                msg = f"The process is running. Waiting until {self.trigger_event}"
                log.info(msg)
                self.save(msg)
            n_iter = n_iter + 1 if n_iter <= 500 else 0
            left_time = self.get_left_time_seconds()
            time.sleep(left_time)


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
        url_to_send = self.generate_url(url_disponibilidad_diaria, self.ini_date, self.end_date)
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
        url_to_send = self.generate_url(url_tags_report, yesterday, self.end_date)
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
        try:
            success1, msg1 = self.process_daily_report()
            success2, msg2 = self.process_tag_report()
            if not success1 or not success2:
                return False, msg1 if not success1 else msg2
            success, msg = self.generate_html_report()
            return success, msg
        except Exception as e:
            msg = "Problemas al procesar la información"
            tb = traceback.format_exc()
            log.error(f"{msg}\n{str(e)}\n{tb}")
            return False, msg

    def process_tag_report(self):
        try:
            self.get_tags_report()
            # si los reportes no han sido procesados y no existen:
            if self.tags_report is None or self.tags_report.empty:
                return False, f"El reporte no puede ser generado debido a que no existe el reporte de tags"
            evaluation_minutes = (self.end_date - self.last_day).total_seconds() / 60
            disponibilidad = [(1-indis/evaluation_minutes)*100 for indis in self.tags_report[lb_indisponible_minutes]]
            self.tags_report[lb_disponibilidad] = disponibilidad
            df_result = pd.DataFrame(columns=[lb_empresa, lb_unidad_negocio, lb_utr_id, lb_utr, lb_protocolo, lb_n_tags, lb_p_tags])
            # Contar las tags que no cumplen con el umbral
            for idx, df in self.tags_report.groupby(by="UTR"):
                umbral = self.parameters[k_disp_tag_umbral]
                mask = df[lb_disponibilidad] < umbral
                df_low = df[mask]
                if df_low.empty:
                    continue
                percentage_tags = round(len(df_low.index) / len(df.index)*100, 2)
                row = {lb_empresa: df[lb_empresa].iloc[0], lb_unidad_negocio: df[lb_unidad_negocio].iloc[0],
                       lb_utr: df[lb_utr].iloc[0], lb_utr_id: df[lb_utr_id].iloc[0],
                       lb_n_tags: len(df_low.index), lb_p_tags: percentage_tags, lb_total_tags: len(df.index)}
                df_result = df_result.append(row, ignore_index=True)
            df_result.sort_values(by=[lb_n_tags], inplace=True)
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
            df_group_resp = self.daily_details.groupby(by=[lb_unidad_negocio, lb_utr, lb_protocolo]).mean()
            df_group_sum = self.daily_details.groupby(by=[lb_unidad_negocio, lb_utr, lb_protocolo]).sum()
            df_group_resp[lb_indisponible_acumulado_minutos] = df_group_sum[lb_indisponible_minutos_promedio]
            df_group_resp.sort_values(by=[lb_dispo_promedio_utr], inplace=True)
            mask = df_group_resp[lb_dispo_promedio_utr] < self.parameters[k_disp_utr_umbral]
            self.to_report_details = df_group_resp[mask]
            n = len(self.daily_summary.index)
            width = 0.5 * n if n > 15 else 1.8 * n
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

    def generate_url(self, url: str, ini_date:dt.datetime, end_date:dt.datetime):
        url_to_send = url.replace("ini_date", ini_date.strftime('%Y-%m-%d'))
        url_to_send = url_to_send.replace("end_date", end_date.strftime('%Y-%m-%d'))
        return url_to_send

    def generate_html_report(self):
        try:
            log.info("Generating the html report")
            html_template_path = os.path.join(init.TEMPLATES_REPO, template_file)
            html_str = codecs.open(html_template_path, 'r', 'utf-8').read()
            utr_item_template = get_block("<!--INI: UTR_DISPONIBILIDAD-->", "<!--FIN: UTR_DISPONIBILIDAD-->", html_str)
            utr_table = str()
            # reportando el estado de las UTRs tiempo acumulado y promedio
            html_str = html_str.replace("(#ini_dd/mm/yyyy)", str(self.ini_date))
            html_str = html_str.replace("(#end_dd/mm/yyyy)", str(self.end_date))
            html_str = html_str.replace("(#no_UTR)", str(len(set(self.daily_details[lb_utr]))))
            n_utrs = 0
            for idx in self.to_report_details.index:
                n_utrs += 1
                u_negocio, utr, protocol = idx
                item = self.to_report_details.loc[idx]
                utr_item = utr_item_template.replace("#U_NEGOCIO", u_negocio)
                utr_item = utr_item.replace("#UTR", utr)
                utr_item = utr_item.replace("#PROTOCOLO", protocol)
                min_acc = item.loc[lb_indisponible_acumulado_minutos]
                utr_item = utr_item.replace("#TIEMPO_ACUMULADO", str(dt.timedelta(minutes=int(min_acc))))
                utr_disp = item.loc[lb_dispo_promedio_utr]
                utr_item = utr_item.replace("#DISP_PROMEDIO", str(round(utr_disp*100, 2)))
                utr_table += utr_item
            html_str = html_str.replace("(#no_UTR_indisponible)", str(n_utrs))
            html_str = html_str.replace("(#_disponibilidad)", str(round(self.parameters[k_disp_utr_umbral]*100,2)))
            html_str = replace_block("<!--INI: UTR_DISPONIBILIDAD-->", "<!--FIN: UTR_DISPONIBILIDAD-->",
                                     html_str, utr_table)

            # colocando links en el reporte:
            yesterday = self.end_date - dt.timedelta(days=1)
            url_to_send = self.generate_url(url_tags_report, yesterday, self.end_date)
            html_str = html_str.replace("#link1a", url_tags_excel)
            html_str = html_str.replace("#link1b", url_tags_json)
            html_str = html_str.replace("#link1c", url_to_send)


            # reportando el estado de las tags:
            html_str = html_str.replace("#disponibilidad_tag", str(round(self.parameters[k_disp_tag_umbral]*100,2)))
            utr_item_template = get_block("<!--INI: TAGS_REPORT-->", "<!--FIN: TAGS_REPORT-->", html_str)
            utr_table = str()
            for idx in self.to_report_tags.index:
                item = self.to_report_tags.loc[idx]
                utr_item = utr_item_template.replace("#U_NEGOCIO", item.loc[lb_unidad_negocio])
                utr_item = utr_item.replace("#UTR", item.loc[lb_utr])
                utr_item = utr_item.replace("#TAGS_INDISP", str(item.loc[lb_n_tags]))
                utr_item = utr_item.replace("#TOTAL_TAGS", str(item.loc[lb_total_tags]))
                utr_item = utr_item.replace("#%TAGS_INDISP", str(item.loc[lb_p_tags]))
                utr_table += utr_item
            html_str = replace_block("<!--INI: TAGS_REPORT-->", "<!--FIN: TAGS_REPORT-->", html_str, utr_table)

            # colocando links en el reporte:
            url_to_send = self.generate_url(url_disponibilidad_diaria, self.ini_date, self.end_date)
            html_str = html_str.replace("#link2a", url_disponibilidad_excel)
            html_str = html_str.replace("#link2b", url_disponibilidad_json)
            html_str = html_str.replace("#link2c", url_to_send)
            save_html(html_str, html_path)
            return True, "El reporte ha sido generado exitosamente"
        except Exception as e:
            msg = "El reporte no ha podido ser generado"
            tb = traceback.format_exc()
            log.error(f"{msg}: \n{str(e)}\n{tb}")
            return False, msg


def test():
    mongo_config = init.MONGOCLIENT_SETTINGS
    connect(**mongo_config)
    rutine_name = "rutina_correo_electronico"
    trigger = dict(hours=7, minutes=40, seconds=0)
    mail_config = dict(from_email="sistemaremoto@cenace.org.ec", users=["rsanchez@cenace.org.ec"],
                       admin=["rsanchez@cenace.org.ec"])
    parameters = dict(disp_utr_umbral=0.9, disp_tag_umbral=0.9)
    ini_date, end_date = get_dates_by_default()
    ini_date, end_date = dt.datetime(year=2021, month=3, day=1), dt.datetime(year=2021, month=3, day=30)
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
