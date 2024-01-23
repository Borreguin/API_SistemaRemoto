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

import asyncio
import multiprocessing
import traceback
from multiprocessing import Process
from typing import Tuple

from app.common import report_log
from app.core.v2CalculationEngine.constants import NUMBER_OF_PROCESSES, TIMEOUT_SECONDS
from app.core.v2CalculationEngine.engine_util import head
from app.core.v2CalculationEngine.node.NodeExecutor import NodeExecutor
from app.db.constants import SR_REPORTE_SISTEMA_REMOTO, V2_SR_FINAL_REPORT_LABEL
from app.db.db_util import *
from app.db.v1.ProcessingState import TemporalProcessingStateReport
from app.db.v2.entities.v2_sRNode import V2SRNode
from app.db.v2.v2SRFinalReport.constants import *
from app.db.v2.v2SRNodeReport.report_util import get_report_id, get_final_report_id, get_general_report_id
from app.main import db_connection


class MasterEngine:
    """ variables generales """
    start_time = dt.datetime.now()
    success = False
    is_already_running = False
    msg = 'No se ha iniciado el cálculo'
    force: bool = False
    final_report: V2SRFinalReportTemporal | V2SRFinalReportPermanent
    general_status: TemporalProcessingStateReport
    general_report_id: str

    """ variables para llevar logs"""
    nodes_msg: List[str] = []
    nodes_report_ids: dict = dict()
    node_error = dict()

    def __init__(self, report_ini_date: dt.datetime, report_end_date: dt.datetime, force=False, permanent_report=False):
        self.is_permanent = permanent_report
        self.all_nodes: List[V2SRNode] = []
        self.report_ini_date = report_ini_date
        self.report_end_date = report_end_date
        self.force = force
        self.results = None
        self.is_already_running = False
        self.node_error = dict()
        self.general_report_id = get_general_report_id(self.is_permanent, self.report_ini_date, self.report_end_date)
        db_connection()

    def check_if_node_is_already_running(self, status: TemporalProcessingStateReport | None, label: str):
        if status is None:
            return False
        if self.force and status is not None and status.finish:
            status.delete()
            return False
        if status is not None:
            running = dt.datetime.now() - status.modified < dt.timedelta(seconds=TIMEOUT_SECONDS) or not status.finish
            assert not running, f"Ya existe un cálculo asociado a {label}. \nFecha [{status.modified}]"
            if status is not None:
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
            is_already_running = self.check_if_node_is_already_running(status, node.nombre)
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
                                                             self.force,
                                                             self.is_permanent
                                                             ))
                                      for node in self.all_nodes]
                results = [res.get(timeout=TIMEOUT_SECONDS) for res in multiple_responses]
                self.success = all([res[0] for res in results])
                self.nodes_msg += [res[1] for res in results]
                head_value = f"{head(self.report_ini_date, self.report_end_date)} "
                success_msg = f"{head_value} Todos los nodos han sido procesados"
                unsuccessful_msg = f"{head_value} No se pudo procesar todos los nodos"
                self.msg = success_msg if self.success else unsuccessful_msg
                self.nodes_msg.append(self.msg)
                report_log.info("\n".join(self.nodes_msg))
                return self.success, self.msg
        except Exception as e:
            msg = f"{head(self.report_ini_date, self.report_end_date)} Error al ejecutar los nodos \n {str(e)}"
            self.nodes_msg.append(msg)
            tb = traceback.format_exc()
            report_log.error(f"{msg} \n{tb}")
            return False, msg

    def create_status_report(self):
        self.general_status = get_temporal_status(self.general_report_id)
        self.check_if_node_is_already_running(self.general_status,
                                              f'Reporte {self.report_ini_date} @ {self.report_end_date}')
        self.general_status = TemporalProcessingStateReport(id_report=self.general_report_id, percentage=0,
                                                            processing=True,
                                                            finish=False, fail=False,
                                                            msg=f"Empezando el cálculo del reporte "
                                                                f"{self.report_ini_date} @ {self.report_end_date}")
        self.general_status.save()

    def finish_status_report(self):
        self.general_status.finished()
        self.general_status.msg = self.msg
        self.general_status.save_safely()

    def run_node(self, node: V2SRNode, id_report: str, ini_report_date: dt.datetime, end_report_date: dt.datetime,
                 force: bool, permanent_report: bool) -> Tuple[bool, str]:
        msg = f"Empezando el cálculo de {node.tipo} {node.nombre}"
        try:
            status = TemporalProcessingStateReport(id_report=id_report, percentage=0, processing=True, msg=msg)
            status.save()
            """ Running the complete process for a single node """
            node_executor = NodeExecutor(node, id_report, ini_report_date, end_report_date, force, permanent_report)
            success, msg = node_executor.processing_node()
            if not success:
                self.node_error[node.nombre] = msg
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
        final_report = get_final_report_v2_by_id(id_report, self.is_permanent)
        if final_report is not None and self.force:
            final_report.delete()
            final_report = None
        assert not (final_report is not None and not self.force), f'Ya existe un reporte previo'
        self.final_report = create_final_report(self.report_ini_date, self.report_end_date, self.is_permanent)

    def calculate_final_report(self):
        assert len(self.nodes_report_ids.keys()) > 0, 'No hay reporte de nodos a procesar'
        nodos_fallidos = 0
        results = dict()
        for id_node, report_id in self.nodes_report_ids.items():
            detail_report_node = get_node_details_report(report_id, self.is_permanent)
            if detail_report_node is None:
                node = V2SRNode.find_by_id(id_node)
                msg = f'No es posible encontrar el nodo con id {id_node}' if node is None else f'No hay reporte detallado para el nodo {node}'
                report_log.warning(msg)
                self.nodes_msg.append(msg)
                nodos_fallidos += 1
                continue
            self.final_report.append_node_detail_report(detail_report_node)
            results[detail_report_node.nombre] = f"Reporte en base de datos: {detail_report_node.actualizado}"
        self.final_report.calculate()
        self.nodes_msg.append(f"El reporte final ha sido calculado exitosamente")
        self.final_report.tiempo_calculo_segundos = (dt.datetime.now() - self.start_time).total_seconds()
        self.final_report.novedades[lb_detalle][lb_fecha_calculo] = str(dt.datetime.now())
        self.final_report.novedades[lb_nodos_fallidos] = nodos_fallidos
        self.final_report.novedades[lb_detalle][lb_log] = self.nodes_msg
        self.final_report.novedades[lb_detalle][lb_results] = results
        self.success, self.msg = save_mongo_document_safely(self.final_report)

    async def calculate_all_active_nodes(self):
        try:
            self.create_status_report()
            self.create_final_report()
            self.get_all_nodes()
            await self.run_all_nodes()
            self.calculate_final_report()
            self.finish_status_report()
            report_log.info(self.msg)
        except Exception as e:
            msg = f'No se pudo calcular el reporte, debido a: {e}'
            report_log.error(f'{msg} \n {traceback.format_exc()}')
            self.success, self.msg = False, msg
            self.general_status.msg = msg
            self.general_status.failed()
            self.general_status.save_safely()


def run_async(master):
    asyncio.run(master.calculate_all_active_nodes())


def run_all_active_nodes(ini_report: dt.datetime, en_report: dt.datetime, force=False, permanent_report=False):
    """ Ejecutando el nodo master """
    master = MasterEngine(ini_report, en_report, force=force, permanent_report=permanent_report)
    process = Process(target=run_async, args=(master,))
    process.start()
    return True, 'Running all active nodes', master.general_report_id

# if __name__ == "__main__":
#     ini_date = dt.datetime.strptime('2023-12-01 00:00:00', '%Y-%m-%d %H:%M:%S')
#     end_date = dt.datetime.strptime('2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
#     asyncio.run(MasterEngine(ini_date, end_date, permanent_report=True, force=True).calculate_all_active_nodes())
#     print('finish')
