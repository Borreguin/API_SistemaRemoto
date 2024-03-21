"""
Desarrollado en la Gerencia de Desarrollo Técnico
by: Roberto Sánchez November 2023
motto:
"Whatever you do, work at it with all your heart, as working for the Lord, not for human master"
Colossians 3:23

2.	Nodo hijo:
•	Recibe parámetros enviados por el nodo master usando la librería argparse: (ubicación del archivo Excel, fecha de cálculo, etc).
•	Lee las configuraciones de nodo: correspondiente al nodo a ejecutar, y establece la fecha a realizar el cálculo.
•	Cada nodo contiene entidades (Subestaciones, centrales, etc). Cada entity_list es procesada mediante un hilo del proceso nodo
•	Cada nodo es leído desde la base de datos MongoDB
•	Una vez ejecutado el nodo, genera un log de su ejecución
•	Guarda en base de datos los resultados, la base de datos se encuentra en “../../_db/mongo_db”

"""
from __future__ import annotations

import os.path
from concurrent.futures import ProcessPoolExecutor
from logging import Logger
from typing import List
import traceback as tb
from app.common import configure_logger
from app.common.default_logger import default_log_path
from app.common.util import get_time_in_minutes, get_time_in_seconds
from app.core.v2CalculationEngine.DatetimeRange import DateTimeRange
from app.core.v2CalculationEngine.engine_util import get_date_time_ranges_using_consignments
from app.core.v2CalculationEngine.node.EntityExecutor import EntityExecutor
from app.core.v2CalculationEngine.util import *
from app.db.constants import attr_nombre, attr_tipo
from app.db.db_util import get_or_create_temporal_report, get_temporal_status
from app.db.v1.ProcessingState import TemporalProcessingStateReport
from app.db.v2.entities.v2_sRConsignment import V2SRConsignment
from app.db.v2.entities.v2_sREntity import V2SREntity
from app.db.v2.entities.v2_sRNode import V2SRNode
from app.db.v2.v2SRNodeReport.V2SRNodeDetailsPermanent import V2SRNodeDetailsPermanent
from app.db.v2.v2SRNodeReport.V2SRNodeDetailsTemporal import V2SRNodeDetailsTemporal
from app.main import db_connection
from app.utils.utils import validate_percentage


class NodeExecutor:
    report_node: V2SRNodeDetailsTemporal | V2SRNodeDetailsPermanent = None
    status_node: TemporalProcessingStateReport = None
    general_status: TemporalProcessingStateReport = None
    pi_svr: PIServerBase = None
    node: V2SRNode = None
    report_id: str = None
    general_report_id: str = None
    executor: ProcessPoolExecutor = None
    entities: List[V2SREntity] = None
    node_consignments: List[V2SRConsignment] = list()
    inner_consignments: List[V2SRConsignment] = list()
    date_time_range: List[DateTimeRange]
    bahias_fallidas: List = list()
    instalaciones_fallidas: List = list()
    entidades_fallidas: List = list()
    numero_bahias_procesadas: int = 0
    numero_instalaciones_procesadas: int = 0
    numero_entidades_procesadas: int = 0
    log: Logger = None

    def __init__(self, node: V2SRNode, report_id: str, general_report_id: str,
                 ini_report_date: dt.datetime, end_report_date: dt.datetime,
                 force: bool, permanent_report: bool):
        self.start_time = dt.datetime.now()
        self.node = node
        self.report_id = report_id
        self.general_report_id = general_report_id
        self.ini_report_date = ini_report_date
        self.end_report_date = end_report_date
        self.force = force
        self.permanent_report = permanent_report
        self.success = False
        self.msg = None
        self.error_code = None
        self.minutes_in_period = None
        self.start_time = dt.datetime.now()
        self.executor = ProcessPoolExecutor()
        self.inner_consignments = list()
        self.consignments = list()
        self.log = configure_logger(log_name=f'{self.node.nombre}.log',
                                    log_path=os.path.join(default_log_path, 'v2Engine'))

    def create_report(self):
        self.report_node = get_node_details_report(self.report_id, self.permanent_report)
        if self.force and self.report_node is not None:
            self.success, self.msg = delete_v2sr_node_report_if_exists(self.report_id, self.permanent_report)
        elif self.report_node is not None and not self.force:
            self.success, self.error_code, self.msg = False, nodeStatus.REPORT_EXIST, f"Ya existe un reporte asociado al nodo {self.node}"
            return False
        self.report_node = create_v2sr_node_report(self.ini_report_date, self.end_report_date, self.node,
                                                   self.permanent_report)

    def get_status_report(self):
        self.log.info(f"get_status_report")
        self.status_node = get_or_create_temporal_report(self.report_id)
        self.status_node.info[attr_nombre] = self.node.nombre
        self.status_node.info[attr_tipo] = self.node.tipo
        self.status_node.update_now()

    def get_general_report(self):
        self.log.info(f"get_general_report")
        self.general_status = get_temporal_status(self.general_report_id)


    def update_info_status_report(self, msg: str, percentage: float):
        self.log.info(msg)
        self.status_node.msg = msg
        self.status_node.percentage = round(percentage, 2)
        self.status_node.save_safely()
        self.general_status.reload()
        self.general_status.msg = msg
        self.general_status.percentage += percentage * 15
        self.general_status.save_safely()

    def update_fail_status_report(self, msg: str):
        self.status_node.failed()
        self.status_node.msg = msg
        self.status_node.save_safely()

    def check_connection(self):
        self.log.info(f"check_connection")
        is_connected = db_connection()
        self.pi_svr = get_pi_server()
        if not is_connected or self.pi_svr is None:
            self.msg = "No se ha podido conectar a la base de datos: "
            self.msg += f"MongoBD" if not is_connected else f"PI Server"
            self.error_code = nodeStatus.NO_DATA_BASE_CONNECTION
            return False
        """ verificar si existe conexión con el servidor PI: """
        self.success, self.error_code = verify_pi_server_connection(self.pi_svr, self.status_node)
        assert self.success, self.error_code

    def get_consignments(self):
        self.log.info(f'[{self.node}] get_consignments')
        self.date_time_range, self.node_consignments = (
            get_date_time_ranges_using_consignments(self.node.get_document_id(), self.ini_report_date,
                                                    self.end_report_date)
        )

    def get_entities(self):
        self.success, self.msg, self.entities = get_active_entities(self.node)

    def init(self):
        self.get_status_report()
        self.get_general_report()
        self.update_info_status_report(f'Get all settings for node {self.node}', 0)
        self.check_connection()
        self.minutes_in_period = get_time_in_minutes(self.ini_report_date, self.end_report_date)
        self.create_report()
        self.get_entities()
        self.get_consignments()
        self.update_info_status_report(f'All settings are ok for node {self.node}', 0)

    def processing_entities(self):
        for ix, entity in enumerate(self.entities):
            try:
                self.update_info_status_report(f'Procesando Entidad: {entity}', ix / len(self.entities))
                min_percentage = validate_percentage(ix / len(self.entities) * 100)
                max_percentage = validate_percentage((ix + 1) / len(self.entities) * 100)
                entity_executor = EntityExecutor(self.pi_svr, entity, self.status_node, self.date_time_range,
                                               self.ini_report_date, self.end_report_date, min_percentage, max_percentage,
                                               self.log).process()
                if not entity_executor.success:
                    self.entidades_fallidas.append(entity.to_summary())
                    continue
                self.report_node.reportes_entidades.append(entity_executor.entity_report)
                self.bahias_fallidas += entity_executor.bahias_fallidas
                self.instalaciones_fallidas += entity_executor.instalaciones_fallidas
                self.numero_bahias_procesadas += entity_executor.numero_bahias_procesadas
                self.numero_instalaciones_procesadas += entity_executor.numero_instalaciones_procesadas
                print("------->", entity_executor.inner_consignments, entity_executor.consignments)
                self.inner_consignments = unique_consignments(self.inner_consignments, entity_executor.inner_consignments + entity_executor.consignments)
                self.numero_entidades_procesadas += 1 if not entity_executor.entity_report.consignada_totalmente else 0
            except Exception as e:
                self.log.error(f'Not able to process a entity: {str(e)} \n{traceback.format_exc()}')
                self.entidades_fallidas.append(entity.to_summary())

        self.report_node.bahias_fallidas = self.bahias_fallidas
        self.report_node.instalaciones_fallidas = self.instalaciones_fallidas
        self.report_node.entidades_fallidas = self.entidades_fallidas
        self.report_node.consignaciones_internas = self.inner_consignments
        self.report_node.calculate()
        self.report_node.tiempo_calculo_segundos = get_time_in_seconds(self.start_time, dt.datetime.now())
        self.log.info(f'[{self.node}] All entities were processed')
        if self.report_node.disponibilidad_promedio_porcentage == -1:
            self.success, self.msg = False, f"No able to calculate this nodo {self.node}"
        else:
            self.success, self.msg = True, f"Saving information for this node"

    def save_report(self):
        self.report_node.numero_bahias_procesadas = self.numero_bahias_procesadas
        self.report_node.numero_instalaciones_procesadas = self.numero_instalaciones_procesadas
        self.report_node.numero_entidades_procesadas = self.numero_entidades_procesadas
        self.report_node.save()


    def processing_node(self):
        try:
            self.init()
            self.processing_entities()
            self.save_report()
            self.msg = f'Nodo [({self.node.tipo}) {self.node.nombre}] fue procesado exitosamente'
            self.success = True
        except Exception as e:
            self.success = False
            self.msg = f'No es posible procesar el nodo {self.node}. \nError: {e}'
            self.update_fail_status_report(self.msg)
            self.status_node.failed()
            self.status_node.save_safely()
            self.log.error(f'{self.msg} {tb.format_exc()}')
        return self.success, self.msg
