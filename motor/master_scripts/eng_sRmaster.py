"""
Desarrollado en la Gerencia de Desarrollo Técnico
by: Roberto Sánchez Febrero 2020
motto:
"Whatever you do, work at it with all your heart, as working for the Lord, not for human master_scripts"
Colossians 3:23

1.	Nodo master:
•	Lee el archivo master.xlsx
•	Define las unidades de negocio a procesar y el directorio donde se encuentra el archivo referente
    a dicha unidad de negocio. Ejemplo: “Unidad Negocio 1 > node_scripts/unidad_de_negocio_1.xls”
•	Define el periodo a la que se ejecutará el cálculo de disponibilidad: inicio (“yyyy-mm-d1”) fin (“yyyy-mm-d2”)
•	Elimina/reescribe los archivos temporales dentro del directorio: “temp”
•	Crea nodos hijos mediante subprocesos distribuidos en los núcleos disponibles
•	Ejecuta el script
    “python eng_sRnode_from_excel_file.py unidad_negocio.xls yyyy-mm-d1 yyyy-mm-d2 -s”
•	Implementa un mecanismo que detecta la finalización de los nodos hijos
•	Guarda en base de datos los resultados, la base de datos se encuentra en “/output/disponibilidad.db”

"""
from dto.mongo_engine_handler.ProcessingState import TemporalProcessingStateReport
from dto.mongo_engine_handler.SRFinalReport.SRFinalReportTemporal import SRFinalReportTemporal
import io, os
import subprocess as sb

from dto.mongo_engine_handler.SRFinalReport.sRFinalReportBase import SRNodeSummaryReport
from dto.mongo_engine_handler.SRNodeReport.SRNodeReportTemporal import SRNodeDetailsTemporal
from dto.mongo_engine_handler.SRNodeReport.sRNodeReportBase import SRNodeDetailsBase
from dto.mongo_engine_handler.SRNodeReport.sRNodeReportPermanente import SRNodeDetailsPermanente
from motor import log_master
from settings import initial_settings as init
from motor.node_scripts.eng_sRnode import eng_results
from my_lib import utils as u

# general variables
script_path = os.path.dirname(os.path.abspath(__file__))
motor_path = os.path.dirname(script_path)
output_path = init.OUTPUT_MOTOR_REPO
node_script = os.path.join(motor_path, 'node_scripts', 'eng_sRnode.py')
debug = init.FLASK_DEBUG
log = log_master

""" Import clases for MongoDB """
from dto.mongo_engine_handler.SRFinalReport.sRFinalReportPermanente import *

""" Time format """
yyyy_mm_dd = "%Y-%m-%d"
yyyy_mm_dd_hh_mm_ss = "%Y-%m-%d %H:%M:%S"
start_time_script = dt.datetime.now()


def run_all_nodes(report_ini_date: dt.datetime, report_end_date: dt.datetime, save_in_db=False, force=False):
    # corre todos los nodos que se encuentran activos:
    global start_time_script
    start_time_script = dt.datetime.now()
    mongo_config = init.MONGOCLIENT_SETTINGS
    try:
        connect(**mongo_config)
    except Exception as e:
        pass

    """ Buscando los nodos con los que se va a trabajar """
    all_nodes = SRNode.objects()
    all_nodes = [n for n in all_nodes if n.activado]
    if len(all_nodes) == 0:
        msg = f"No hay nodos a procesar en db:[{mongo_config['db']}]"
        return False, [], msg
    name_list = [n.nombre for n in all_nodes]
    for sR_node in all_nodes:
        report_node = SRNodeDetailsBase(nodo=sR_node, nombre=sR_node.nombre, tipo=sR_node.tipo,
                                        fecha_inicio=report_ini_date,
                                        fecha_final=report_end_date)
        status_node = TemporalProcessingStateReport.objects(id_report=report_node.id_report).first()
        if status_node is not None:
            status_node.delete()

    return run_node_list(name_list, report_ini_date, report_end_date, save_in_db, force)


def run_node_list(node_list_name: list, report_ini_date: dt.datetime, report_end_date: dt.datetime,
                  save_in_db=False, force=False):
    # cálcula disponibilidad por cada nodo
    global start_time_script
    start_time_script = dt.datetime.now()

    """ variables para llevar logs"""
    msg = list()
    results = dict()
    log.info(f"[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] Empezando el cálculo de los nodos: {node_list_name}")

    """ check si es una lista de strings """
    check = all(isinstance(li, str) for li in node_list_name)
    if not check:
        return False, results, "La lista 'node_list_name' no es una lista válida de nombre de nodoss"

    try:
        child_processes = list()
        for node in node_list_name:

            # Procesando cada nodo individual:
            # p es un proceso ejecutado
            success, p, _msg = executing_node(node, report_ini_date, report_end_date, save_in_db, force)
            if success:
                log.info(_msg)
                msg.append(_msg)
                child_processes.append(p)

        # now we can join them together
        # uniendo todos los procesos juntos:
        _msg = f"[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] Procesando todos los nodos:"
        log.info(_msg)
        msg.append(_msg)
        # recollecting results from child processes
        success, results, _msg = collecting_results_from(child_processes)
        if success:
            msg += _msg
            return success, results, msg
        else:
            msg += _msg
            return success, results, msg

    except Exception as e:
        msg = f"No se pudo procesar todos los nodos \n [{str(e)}]\n [{traceback.format_exc()}]"
        log.error(msg)
        return False, None, msg


def executing_node(node, report_ini_date, report_end_date, save_in_db, force):
    try:
        # Procesando cada nodo individual:
        log_path = os.path.join(output_path, f'{node}.log')
        with io.open(log_path, mode='wb') as out:
            to_run = ["python", node_script, node, report_ini_date.strftime(yyyy_mm_dd),
                      report_end_date.strftime(yyyy_mm_dd)]
            if save_in_db:
                to_run += ["--s"]
            if force:
                to_run += ["--f"]
            # empezando un proceso a la vez
            p = sb.Popen(to_run, stdout=out, stderr=out)
            msg = f"->[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] Ejecutando: [{valid_name(node)}] " \
                  f"save_in_db={save_in_db} force={force}"
            return True, p, msg
    except Exception as e:
        msg = f"Error al ejecutar el nodo: [{node}] \n {str(e)}"
        tb = traceback.extract_stack()
        log.error(f"{msg} \n {tb}")
        return False, None, msg


def collecting_results_from(child_processes):
    # Por cada proceso se recoje el resultado:
    try:
        results = dict()
        msg = list()
        fails = 0
        for cp in child_processes:
            cp.wait()
            # para dar seguimiento a los resultados:
            to_print = f"<-[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] (#st) Finalizando el nodo [{valid_name(cp.args[2])}] "
            if cp.returncode in [0, 9, 10]:
                to_print = to_print.replace("#st", "OK   ")
            elif cp.returncode in [8]:
                to_print = to_print.replace("#st", "WARN ")
            else:
                to_print = to_print.replace("#st", "ERROR")
                fails += 1
            # details para identificar como finalizó el cálculo
            # eng_results es un diccionario con los resultados posibles
            details = [d[1] for d in eng_results if d[0] == cp.returncode]
            # cp.args[2] es el nombre del proceso
            nombre_proceso = valid_name(str(cp.args[2]))
            results.update({nombre_proceso: details[0]})
            log.info(to_print)
            msg.append(to_print)
        to_print = f"[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] Finalizando todos los nodos \n"
        log.info(to_print)
        msg.append(to_print)
        if fails == len(child_processes):
            return False, results, msg
        return True, results, msg
    except Exception as e:
        _msg = f"Error al recopilar los resultados de los nodos"
        tb = traceback.extract_stack()
        log.error(f"{_msg} \n {tb}")
        return False, None, [_msg]


def run_summary(report_ini_date: dt.datetime, report_end_date: dt.datetime, save_in_db=False, force=False,
                results=None, log_msg=None):
    # Connect if is needed
    mongo_config = init.MONGOCLIENT_SETTINGS
    try:
        connect(**mongo_config)
    except Exception as e:
        print(e)
    log.info("Empezando el cálculo del reporte final")
    # Verificando si debe usar el reporte temporal o definitivo:
    isTemporalReport = u.isTemporal(report_ini_date, report_end_date)
    if isTemporalReport:
        final_report = SRFinalReportTemporal(fecha_inicio=report_ini_date, fecha_final=report_end_date)
        final_report_v = SRFinalReportTemporal.objects(id_report=final_report.id_report).first()
    else:
        final_report = SRFinalReportPermanente(fecha_inicio=report_ini_date, fecha_final=report_end_date)
        final_report_v = SRFinalReportPermanente.objects(id_report=final_report.id_report).first()

    report_exists = final_report_v is not None
    if save_in_db and not force and report_exists:
        msg = "El reporte ya existe en base de datos"
        log.info(msg)
        return False, final_report_v, msg

    """ Buscando los nodos con los que se va a trabajar """
    all_nodes = SRNode.objects()
    all_nodes = [n for n in all_nodes if n.activado]
    for node in all_nodes:
        if isTemporalReport:
            report_v = SRNodeDetailsTemporal(nombre=node.nombre, tipo=node.tipo, fecha_inicio=report_ini_date,
                                             fecha_final=report_end_date)
            report = SRNodeDetailsTemporal.objects(id_report=report_v.id_report).first()
        else:
            report_v = SRNodeDetailsPermanente(nombre=node.nombre, tipo=node.tipo, fecha_inicio=report_ini_date,
                                               fecha_final=report_end_date)
            report = SRNodeDetailsPermanente.objects(id_report=report_v.id_report).first()
        if report is None:
            final_report.novedades["nodos_fallidos"] += 1
            if not "nodos" in final_report.novedades["detalle"]:
                final_report.novedades["detalle"]["nodos"] = list()
            final_report.novedades["detalle"]["nodos"].append(dict(tipo=node.tipo, nombre=node.nombre))
            continue
        node_summary_report = SRNodeSummaryReport(**report.to_summary())
        # final_report.reportes_nodos_detalle.append(report)
        final_report.append_node_summary_report(node_summary_report)

    # añadiendo novedades encontradas al momento de realizar el cálculo nodo por nodo:
    final_report.novedades["detalle"]["log"] = log_msg
    final_report.novedades["detalle"]["results"] = results
    final_report.calculate()
    if report_exists and force:
        if "log" in final_report_v.novedades["detalle"].keys():
            final_report.novedades["detalle"]["log_previo"] = final_report_v.novedades["detalle"]["log"]
        if "results" in final_report_v.novedades["detalle"].keys():
            final_report.novedades["detalle"]["resultado_previo"] = final_report_v.novedades["detalle"]["results"]
        final_report_v.delete()
    delta_time = dt.datetime.now() - start_time_script
    final_report.actualizado = dt.datetime.now()
    final_report.tiempo_calculo_segundos = delta_time.total_seconds()
    log.info("Guardando reporte final en base de datos...")
    # log.info(final_report.to_dict())
    # Save in database
    try:
        final_report.save()
        msg = "El reporte ha sido calculado exitosamente"
        log.info(msg)
        return True, final_report, msg
    except Exception as e:
        msg = f"Problemas al guardar el reporte \n{str(e)}"
        log.info(msg)
        return False, final_report, "El reporte no ha sido guardado"


def valid_name(name):
    valid_name = str(name).replace(".", "_")
    valid_name = valid_name.replace("$", "_")
    return valid_name


def test():
    report_ini_date, report_end_date = u.get_dates_for_last_month()
    report_ini_date, report_end_date = dt.datetime(2020, 10, 11), dt.datetime(2020, 10, 12)
    success, results, msg = run_all_nodes(report_ini_date, report_end_date, save_in_db=True, force=True)
    run_summary(report_ini_date, report_end_date, force=True, results=results, log_msg=msg)


if __name__ == "__main__":

    if debug:
        test()
