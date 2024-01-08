from __future__ import annotations

import traceback
from logging import Logger
from typing import List, Tuple
import datetime as dt

from app.common import error_log
from app.common.PI_connection.PIServer.PIServerBase import PIServerBase
from app.common.util import get_time_in_minutes
from app.core.v2CalculationEngine.DatetimeRange import DateTimeRange, get_total_time_in_minutes
from app.core.v2CalculationEngine.engine_util import get_date_time_ranges_from_consignment_time_ranges
from app.core.v2CalculationEngine.node.node_util import processing_unavailability_of_tags
from app.db.v1.ProcessingState import TemporalProcessingStateReport
from app.db.v2.entities.v2_sRBahia import V2SRBahia
from app.db.v2.entities.v2_sRConsignment import V2SRConsignment
from app.db.v2.entities.v2_sREntity import V2SREntity
from app.db.v2.entities.v2_sRInstallation import V2SRInstallation
from app.db.v2.v2SRNodeReport.Details.v2_sRBahiaReportDetails import V2SRBahiaReportDetails
from app.db.v2.v2SRNodeReport.Details.v2_sREntityReportDetails import V2SREntityReportDetails
from app.db.v2.v2SRNodeReport.Details.v2_sRInstallationReportDetails import V2SRInstallationReportDetails


class EntityExecutor:
    pi_svr: PIServerBase = None
    status_node: TemporalProcessingStateReport = None
    entity_report: V2SREntityReportDetails
    minutes_in_period: int
    consignments: List[V2SRConsignment] = list()
    msgs: List[str] = list()
    entity_time_ranges: List[DateTimeRange] = list()
    bahias_fallidas: List = list()
    instalaciones_fallidas: List = list()
    numero_bahias_procesadas: int = 0
    numero_instalaciones_procesadas: int = 0
    log: Logger = None

    def __init__(self, pi_svr: PIServerBase, entity: V2SREntity, status_node: TemporalProcessingStateReport,
                 node_ranges: List[DateTimeRange], ini_report_date: dt.datetime, end_report_date: dt.datetime,
                 min_percentage: float, max_percentage: float, log: Logger):
        self.success = False
        self.pi_svr = pi_svr
        self.entity = entity
        self.status_node = status_node
        self.node_ranges = node_ranges
        self.min_percentage = min_percentage
        self.max_percentage = max_percentage
        self.ini_report_date = ini_report_date
        self.end_report_date = end_report_date
        self.minutes_in_period = get_time_in_minutes(self.ini_report_date, self.end_report_date)
        self.log = log

    def update_fail_status_report(self, msg: str):
        self.log.error(msg)
        self.msgs.append(msg)
        self.status_node.failed()
        self.status_node.msg = msg
        self.status_node.save_safely()

    def update_info_status_report(self, msg: str, percentage: float):
        self.log.info(msg)
        self.msgs.append(msg)
        self.status_node.msg = msg
        self.status_node.percentage = round(percentage, 2)
        self.status_node.save_safely()

    def get_percentage(self, ix):
        percentage_delta = (self.max_percentage - self.min_percentage) / (len(self.entity.instalaciones))
        return self.min_percentage + ix * percentage_delta

    def create_entity_report(self):
        self.entity_report = V2SREntityReportDetails()
        self.entity_report.set_values(self.entity)

    def get_entity_consignments(self):
        entity_time_ranges, entity_consignments = (
            get_date_time_ranges_from_consignment_time_ranges(self.entity.get_document_id(), self.node_ranges)
        )
        self.consignments += entity_consignments
        return entity_time_ranges

    def validate_entities(self):
        if self.entity.instalaciones is None or len(self.entity.instalaciones) == 0:
            self.update_fail_status_report(f"No hay instalaciones a procesar para esta entidad {self.entity}")
        assert len(self.entity.instalaciones) > 0, f"Se requiere instalaciones en entidad {self.entity}"

    def create_installation_report_and_get_bahias_to_process(self, installation, ix: int) \
            -> Tuple[bool, List[V2SRBahia], V2SRInstallationReportDetails | None, List[DateTimeRange] | None]:
        installation_db: V2SRInstallation = installation.fetch()
        self.update_info_status_report(f"Procesando instalación {installation_db}", self.get_percentage(ix))
        inst_time_ranges, inst_consignments = (
            get_date_time_ranges_from_consignment_time_ranges(installation_db.get_document_id(), self.entity_time_ranges)
        )
        self.consignments += inst_consignments
        if installation_db.bahias is None or len(installation_db.bahias) == 0:
            msg = f"No hay bahias a procesar para esta instalación {installation_db}"
            self.update_info_status_report(msg, self.get_percentage(ix))
            return False, [], None, None
        installation_report = V2SRInstallationReportDetails()
        installation_report.set_values(installation_db, inst_consignments)
        return True, installation_db.bahias, installation_report, inst_time_ranges

    def process_bahia(self, bahia: V2SRBahia, installation_time_ranges: List[DateTimeRange]) -> Tuple[bool, str, None|V2SRBahiaReportDetails]:
        bahia_time_ranges, bahia_consignments_list = (
            get_date_time_ranges_from_consignment_time_ranges(bahia.get_document_id(), installation_time_ranges)
        )
        self.consignments += bahia_consignments_list
        if bahia.tags is None or len(bahia.tags) == 0:
            return False, f"No hay tags a procesar para bahia {bahia}", None
        bahia_report = V2SRBahiaReportDetails()
        bahia_report.set_values(bahia, self.minutes_in_period, bahia_consignments_list)
        success, msg, tag_reports, failed_tags = processing_unavailability_of_tags(bahia.tags, bahia_time_ranges, self.pi_svr)
        if not success or len(failed_tags) == len(bahia.tags):
            self.bahias_fallidas.append(bahia.to_summary())
            return False, msg, None
        self.numero_bahias_procesadas += 1
        bahia_report.periodo_efectivo_minutos = get_total_time_in_minutes(bahia_time_ranges)
        bahia_report.reportes_tags = tag_reports
        bahia_report.tags_fallidas = failed_tags
        bahia_report.calculate()
        return True, f'Reporte de Bahia calculado', bahia_report

    def process_bahias(self, installation, ix):
        self.success, bahia_list, installation_report, installation_time_ranges = (
            self.create_installation_report_and_get_bahias_to_process(installation, ix)
        )
        for bahia in bahia_list:
            success, msg, bahia_report = self.process_bahia(bahia, installation_time_ranges)
            if not success:
                self.msgs.append(msg)
                continue
            installation_report.reportes_bahias.append(bahia_report)
            installation_report.tags_fallidas += bahia_report.tags_fallidas

        installation_report.periodo_evaluacion_minutos = get_total_time_in_minutes(installation_time_ranges)
        installation_report.calculate()
        if installation_report.disponibilidad_promedio_porcentage == -1:
            self.instalaciones_fallidas.append(installation.fetch().to_summary())
        self.numero_instalaciones_procesadas += 1
        self.entity_report.reportes_instalaciones.append(installation_report)


    def process(self):
        self.validate_entities()
        self.entity_time_ranges = self.get_entity_consignments()
        self.create_entity_report()
        for ix, installation in enumerate(self.entity.instalaciones):
            try:
                self.process_bahias(installation, ix)
            except Exception as e:
                msg = f'Not able to process an installation: {str(e)}'
                error_log.error(f'{msg} \n{traceback.format_exc()}')

        self.entity_report.periodo_evaluacion_minutos = get_total_time_in_minutes(self.entity_time_ranges)
        self.entity_report.calculate()
        return self