"""
Desarrollado en la Gerencia de Desarrollo Técnico
by: Roberto Sánchez November 2023 - version v2
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
from __future__ import annotations

import multiprocessing
import traceback
from typing import Tuple

from app.common import report_log
from app.core.v2CalculationEngine.constants import NUMBER_OF_PROCESSES, TIMEOUT_SECONDS
from app.core.v2CalculationEngine.engine_util import head
from app.core.v2CalculationEngine.node.NodeExecutor import NodeExecutor
from app.db.constants import SR_REPORTE_SISTEMA_REMOTO
from app.db.db_util import *
from app.db.v1.ProcessingState import TemporalProcessingStateReport
from app.db.v2.entities.v2_sRNode import V2SRNode
from app.db.v2.v2SRNodeReport.report_util import get_report_id, get_final_report_id
from app.main import db_connection


class MasterEngine:
    """ variables generales """
    start_time = dt.datetime.now()
    success = False
    is_already_running = False
    msg = 'No se ha iniciado el cálculo'
    force: bool = False
    final_report: V2SRFinalReportTemporal | V2SRFinalReportPermanent

    """ variables para llevar logs"""
    nodes_msg: List[str] = []
    nodes_results: dict = dict()
    nodes_report_ids: dict = dict()

    def __init__(self, report_ini_date: dt.datetime, report_end_date: dt.datetime,
                 save_in_db=False, force=False, permanent_report=False):
        self.is_permanent = permanent_report
        self.all_nodes: List[V2SRNode] = []
        self.report_ini_date = report_ini_date
        self.report_end_date = report_end_date
        self.save_in_db = save_in_db
        self.force = force
        self.results = None
        self.is_already_running = False
        db_connection()

    def check_if_node_is_already_running(self, status: TemporalProcessingStateReport | None, node: V2SRNode):
        if status is None:
            return False
        if dt.datetime.now() - status.modified < dt.timedelta(seconds=TIMEOUT_SECONDS):
            self.is_already_running = True
            self.success, self.msg = False, f"El nodo {node.nombre} ya se está procesando"
            return True
        else:
            status.delete()
            return False

    def get_all_nodes(self):
        """ Buscando los nodos con los que se va a trabajar """
        all_nodes = get_all_v2_nodes(active=True)
        if len(all_nodes) == 0:
            self.success, self.msg = False, f"No hay nodos a procesar"
        nodes = list()
        for node in all_nodes:
            nodes.append(node)
            report_id = get_report_id(node.tipo, node.nombre, self.report_ini_date, self.report_end_date)
            self.nodes_report_ids[node.id_node] = report_id
            status = get_temporal_status(report_id)
            is_already_running = self.check_if_node_is_already_running(status, node)
            if is_already_running:
                return False, self.msg

        self.all_nodes = nodes
        self.success, self.msg = True, f"Se encontraron {len(all_nodes)} nodos a procesar"
        return True, self.msg

    async def run_all_nodes(self):
        if not self.success or self.is_already_running:
            return False, f"Existen nodos que están siendo procesados"
        msg = f"{head(self.report_ini_date, self.report_end_date)} Empezando el cálculo {len(self.all_nodes)} nodos"
        report_log.info(msg)
        self.nodes_msg.append(msg)
        try:
            with multiprocessing.Pool(processes=NUMBER_OF_PROCESSES) as pool:
                multiple_responses = [pool.apply_async(self.run_node,
                                                       args=(node,
                                                             self.nodes_report_ids[node.id_node],
                                                             self.report_ini_date,
                                                             self.report_end_date,
                                                             self.save_in_db,
                                                             self.force,
                                                             self.is_permanent
                                                             ))
                                      for node in self.all_nodes]
                results = [res.get(timeout=TIMEOUT_SECONDS) for res in multiple_responses]
                self.success = all([res[0] for res in results])
                self.nodes_msg += [res[1] for res in results]
                head_value = f"{head(self.report_ini_date, self.report_end_date)} "
                success_msg = f"{head_value} Todos los nodos han sido procesados"
                unsuccessful_msg = f"{head_value} No se pudo procesar todos los nodos \nDetalles:\n" + "\n".join(self.nodes_msg)
                self.msg = success_msg if self.success else unsuccessful_msg
                self.nodes_msg.append(self.msg)
                return self.success, self.msg
        except Exception as e:
            msg = f"{head(self.report_ini_date, self.report_end_date)} Error al ejecutar los nodos \n {str(e)}"
            self.nodes_msg.append(msg)
            tb = traceback.extract_stack()
            report_log.error(f"{msg} \n{tb}")
            return False, msg

    @staticmethod
    def run_node(node: V2SRNode, id_report: str, ini_report_date: dt.datetime, end_report_date: dt.datetime,
                 save_in_db: bool, force: bool, permanent_report: bool) -> Tuple[bool, str]:
        msg = f"Empezando el cálculo de {node.tipo} {node.nombre}"
        try:
            status = TemporalProcessingStateReport(id_report=id_report, percentage=0, processing=True, msg=msg)
            status.save()
            """ Running the complete process for a single node """
            node_executor = NodeExecutor(node, id_report, ini_report_date, end_report_date, save_in_db, force, permanent_report)
            success, msg = node_executor.processing_node()
            report_log.info(head(ini_report_date, end_report_date) + msg)
        except Exception as e:
            msg = f"Error al procesar el nodo {node}:\n{str(e)}"
            report_log.error(f'{msg} {traceback.format_exc()}')
            success, msg = False, msg
        status = get_temporal_status(id_report)
        status.delete()
        return success, msg

    def create_final_report(self):
        id_report = get_final_report_id(SR_REPORTE_SISTEMA_REMOTO, self.report_ini_date, self.report_end_date)
        final_report = get_final_report_by_id(id_report, self.is_permanent)
        assert not(self.force and final_report is not None) , 'Ya existe un reporte previo'
        if final_report is not None and self.force:
            final_report.delete()
        self.final_report = create_final_report(self.report_ini_date, self.final_report, self.is_permanent)

    def calculate_final_report(self):
        assert len(self.nodes_report_ids.keys()) > 0, 'No hay reporte de nodos a procesar'
        for id_node, report_id in self.nodes_report_ids.items():
            detail_report_node = get_node_details_report(report_id, self.is_permanent)
            if detail_report_node is None:
                node = V2SRNode.find_by_id(id_node)
                msg = f'No es posible encontrar el nodo con id {id_node}' if node is None else f'No hay reporte detallado para el nodo {node}'
                report_log.warning(msg)
                self.nodes_msg.append(msg)
            self.final_report.append_each_node_detail()


    def calculate_all_active_nodes(self, force: bool = False):
        try:
            self.force = force
            self.create_final_report()
            self.get_all_nodes()
            self.run_all_nodes()
            report_log.info(self.msg)
        except Exception as e:
            report_log.error(f'No able to calculate_all_active_nodes due to {e} \n {traceback.extract_stack()}')


if __name__ == "__main__":
    ini_date = dt.datetime.strptime('2023-10-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    end_date = dt.datetime.strptime('2023-10-30 00:00:00', '%Y-%m-%d %H:%M:%S')
    MasterEngine(ini_date, end_date).calculate_all_active_nodes(force=False)
    print('finish')


# TODO: RS Check this code down
#             child_processes = list()
#             for node in self.all_nodes:
#                 # Procesando cada nodo individual:
#                 # p es un proceso ejecutado
#                 success, p, _msg = executing_node(node, self.report_ini_date, self.report_end_date, save_in_db, force)
#                 if success:
#                     report_log.info(_msg)
#                     self.nodes_msg.append(_msg)
#                     child_processes.append(p)
#
#             # now we can join them together
#             # uniendo todos los procesos juntos:
#             _msg = f"{head(self.report_ini_date, self.report_end_date)} Procesando todos los nodos:"
#             report_log.info(_msg)
#             self.nodes_msg.append(_msg)
#             # recollecting results from child processes
#             success, results, _msg = collecting_results_from(child_processes)
#             report_log.info(f"{head(self.report_ini_date, self.report_end_date)} Se recolecta todos los reportes")
#             if success:
#                 self.nodes_msg += _msg
#                 self.results = results
#                 return success, results, self.nodes_msg
#             else:
#                 self.nodes_msg += _msg
#                 return success, results, self.nodes_msg
#
#
#
#
#
#     try:
#         child_processes = list()
#         for node in node_list_name:
#
#             # Procesando cada nodo individual:
#             # p es un proceso ejecutado
#             success, p, _msg = executing_node(node, report_ini_date, report_end_date, save_in_db, force)
#             if success:
#                 log.info(_msg)
#                 msg.append(_msg)
#                 child_processes.append(p)
#
#         # now we can join them together
#         # uniendo todos los procesos juntos:
#         _msg = f"{head(report_ini_date, report_end_date)} Procesando todos los nodos:"
#         log.info(_msg)
#         msg.append(_msg)
#         # recollecting results from child processes
#         success, results, _msg = collecting_results_from(child_processes)
#         log_master.info(f"{head(report_ini_date, report_end_date)} Se recolecta todos los reportes")
#         if success:
#             msg += _msg
#             return success, results, msg
#         else:
#             msg += _msg
#             return success, results, msg
#
#     except Exception as e:
#         msg = f"{head(report_ini_date, report_end_date)} No se pudo procesar todos los nodos " \
#               f"\n [{str(e)}]\n [{traceback.format_exc()}] "
#         log.error(msg)
#         return False, None, msg
#
#
# def executing_node(node, report_ini_date, report_end_date, save_in_db, force):
#     try:
#         # Procesando cada nodo individual:
#         log_path = os.path.join(output_path, f'{node}.log')
#         with io.open(log_path, mode='wb') as out:
#             to_run = ["python", node_script, node, report_ini_date.strftime(yyyy_mm_dd_hh_mm_ss),
#                       report_end_date.strftime(yyyy_mm_dd_hh_mm_ss)]
#             if save_in_db:
#                 to_run += ["--s"]
#             if force:
#                 to_run += ["--f"]
#             # empezando un proceso a la vez
#             p = sb.Popen(to_run, stdout=out, stderr=out)
#             msg = f"->{head(report_ini_date, report_end_date)} Ejecutando: [{valid_name(node)}] " \
#                   f"save_in_db={save_in_db} force={force}"
#             return True, p, msg
#     except Exception as e:
#         msg = f"{head(report_ini_date, report_end_date)} Error al ejecutar el nodo: [{node}] \n {str(e)}"
#         tb = traceback.extract_stack()
#         log.error(f"{msg} \n {tb}")
#         return False, None, msg
#
#
# def collecting_results_from(child_processes):
#     # Por cada proceso se recoje el resultado:
#     try:
#         results = dict()
#         msg = list()
#         fails = 0
#         for cp in child_processes:
#             cp.wait()
#             # para dar seguimiento a los resultados:
#             to_print = f"<-[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] " \
#                        f"(#st) Finalizando el nodo [{valid_name(cp.args[2])}] "
#             if cp.returncode in [0, 9, 10]:
#                 to_print = to_print.replace("#st", "OK   ")
#             elif cp.returncode in [8]:
#                 to_print = to_print.replace("#st", "WARN ")
#             else:
#                 to_print = to_print.replace("#st", "ERROR")
#                 fails += 1
#             # details para identificar como finalizó el cálculo
#             # eng_results es un diccionario con los resultados posibles
#             details = [d[1] for d in eng_results if d[0] == cp.returncode]
#             # cp.args[2] es el nombre del proceso
#             nombre_proceso = valid_name(str(cp.args[2]))
#             results.update({nombre_proceso: details[0]})
#             log.info(to_print)
#             msg.append(to_print)
#         to_print = f"[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] Finalizando todos los nodos \n"
#         log.info(to_print)
#         msg.append(to_print)
#         if fails == len(child_processes):
#             return False, results, msg
#         return True, results, msg
#     except Exception as e:
#         _msg = f"Error al recopilar los resultados de los nodos"
#         tb = traceback.extract_stack()
#         log.error(f"{_msg} \n {tb}")
#         return False, None, [_msg]
#
#
# def run_summary(report_ini_date: dt.datetime, report_end_date: dt.datetime, save_in_db=False, force=False,
#                 results=None, log_msg=None):
#     # Connect if is needed
#     mongo_config = init.MONGOCLIENT_SETTINGS
#     try:
#         connect(**mongo_config)
#     except Exception as e:
#         print(e)
#     log.info(f"{head(report_ini_date, report_end_date)} Empezando el cálculo del reporte final")
#     # Verificando si debe usar el reporte temporal o definitivo:
#     isTemporalReport = u.isTemporal(report_ini_date, report_end_date)
#     if isTemporalReport:
#         final_report_v = SRFinalReportTemporal(fecha_inicio=report_ini_date, fecha_final=report_end_date)
#         final_report_db = SRFinalReportTemporal.objects(id_report=final_report_v.id_report).first()
#     else:
#         final_report_v = SRFinalReportPermanente(fecha_inicio=report_ini_date, fecha_final=report_end_date)
#         final_report_db = SRFinalReportPermanente.objects(id_report=final_report_v.id_report).first()
#
#     report_exists = final_report_db is not None
#     if save_in_db and not force and report_exists:
#         msg = "El reporte ya existe en base de datos"
#         log.info(msg)
#         return False, final_report_db, msg
#
#     """ Buscando los nodos con los que se va a trabajar """
#     all_nodes = SRNode.objects()
#     all_nodes = [n for n in all_nodes if n.activado]
#     for node in all_nodes:
#         report_v = SRNodeDetailsBase(nombre=node.nombre, tipo=node.tipo, fecha_inicio=report_ini_date,
#                                      fecha_final=report_end_date)
#         if isTemporalReport:
#             report = SRNodeDetailsTemporal.objects(id_report=report_v.id_report).first()
#         else:
#             report = SRNodeDetailsPermanente.objects(id_report=report_v.id_report).first()
#
#         if report is None:
#             final_report_v.novedades["nodos_fallidos"] += 1
#             if not "nodos" in final_report_v.novedades["detalle"]:
#                 final_report_v.novedades["detalle"]["nodos"] = list()
#             final_report_v.novedades["detalle"]["nodos"].append(dict(tipo=node.tipo, nombre=node.nombre))
#             continue
#         node_summary_report = SRNodeSummaryReport(**report.to_summary())
#         # final_report.reportes_nodos_detalle.append(report)
#         final_report_v.append_node_summary_report(node_summary_report)
#
#     # añadiendo novedades encontradas al momento de realizar el cálculo nodo por nodo:
#     final_report_v.novedades["detalle"]["log"] = log_msg
#     final_report_v.novedades["detalle"]["results"] = results
#     final_report_v.calculate()
#     if report_exists and force:
#         if "log" in final_report_db.novedades["detalle"].keys():
#             final_report_v.novedades["detalle"]["log_previo"] = final_report_db.novedades["detalle"]["log"]
#         if "results" in final_report_db.novedades["detalle"].keys():
#             final_report_v.novedades["detalle"]["resultado_previo"] = final_report_db.novedades["detalle"]["results"]
#         final_report_db.delete()
#     delta_time = dt.datetime.now() - start_time_script
#     final_report_v.actualizado = dt.datetime.now()
#     final_report_v.tiempo_calculo_segundos = delta_time.total_seconds()
#     log.info(f"{head(report_ini_date, report_end_date)} Guardando reporte final en base de datos...")
#     # log.info(final_report.to_dict())
#     # Save in database
#     try:
#         final_report_v.save()
#         msg = f"{head(report_ini_date, report_end_date)} El reporte ha sido calculado exitosamente"
#         log.info(msg)
#         return True, final_report_v, msg
#     except Exception as e:
#         msg = f"{head(report_ini_date, report_end_date)} Problemas al guardar el reporte \n{str(e)}"
#         log.info(msg)
#         return False, final_report_v, "El reporte no ha sido guardado"
#
#
# def run_nodes_and_summarize(report_ini_date, report_end_date, save_in_db, force):
#     success, results, msg = run_all_nodes(report_ini_date, report_end_date, save_in_db, force)
#     if not success:
#         return success, None, msg
#     success, final_report, msg = run_summary(report_ini_date, report_end_date, save_in_db=save_in_db, force=force,
#                                              results=results, log_msg=msg)
#     return success, final_report, msg
#
#
# def head(ini: dt.datetime, end: dt.datetime):
#     return f"[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] ({ini}@{end})"
#
#
# def valid_name(name):
#     valid_name = str(name).replace(".", "_")
#     valid_name = valid_name.replace("$", "_")
#     return valid_name
#
#
# def test():
#     report_ini_date, report_end_date = u.get_dates_for_last_month()
#     report_ini_date, report_end_date = dt.datetime(2020, 10, 11), dt.datetime(2020, 10, 12)
#     success, results, msg = run_all_nodes(report_ini_date, report_end_date, save_in_db=True, force=True)
#     run_summary(report_ini_date, report_end_date, force=True, results=results, log_msg=msg)

