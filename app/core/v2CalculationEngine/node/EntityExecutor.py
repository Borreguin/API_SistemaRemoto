from logging import Logger
from typing import List

from app.common import error_log
from app.common.PI_connection.PIServer.PIServerBase import PIServerBase
from app.core.v2CalculationEngine.DatetimeRange import DateTimeRange
from app.core.v2CalculationEngine.engine_util import get_date_time_ranges_from_consignment_time_ranges
from app.core.v2CalculationEngine.node.node_util import processing_unavailability_of_tags
from app.db.v1.ProcessingState import TemporalProcessingStateReport
from app.db.v2.entities.v2_sRConsignment import V2SRConsignment
from app.db.v2.entities.v2_sREntity import V2SREntity
from app.db.v2.entities.v2_sRInstallation import V2SRInstallation
from app.db.v2.v2SRNodeReport.Details.v2_sREntityReportDetails import V2SREntityReportDetails
from app.db.v2.v2SRNodeReport.Details.v2_sRInstallationReportDetails import V2SRInstallationReportDetails


class EntityExecutor:
    pi_svr: PIServerBase = None
    status_node: TemporalProcessingStateReport = None
    entity_report: V2SREntityReportDetails
    minutes_in_period: int
    consignments: List[V2SRConsignment]
    msgs: List[str]
    entity_time_ranges: List[DateTimeRange]
    log: Logger = None

    def __init__(self, pi_svr: PIServerBase, entity: V2SREntity, status_node: TemporalProcessingStateReport,
                 node_ranges: List[DateTimeRange], minutes_in_period: int,
                 min_percentage: float, max_percentage: float, log: Logger):
        self.pi_svr = pi_svr
        self.entity = entity
        self.status_node = status_node
        self.node_ranges = node_ranges
        self.min_percentage = min_percentage
        self.max_percentage = max_percentage
        self.minutes_in_period = minutes_in_period
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
        percentage_delta = (self.max_percentage - self.min_percentage)/(len(self.entity.instalaciones))
        return self.min_percentage + ix * percentage_delta * 100

    def create_entity_report(self):
        self.entity_report = V2SREntityReportDetails(self.entity, self.minutes_in_period)

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

    def create_installation_report_and_get_bahias_to_process(self, installation, ix:int):
        instalacion: V2SRInstallation = installation.fetch()
        self.update_info_status_report(f"Procesando instalación {instalacion}", self.get_percentage(ix))
        inst_time_ranges, inst_consignments = (
            get_date_time_ranges_from_consignment_time_ranges(instalacion.get_document_id(), self.entity_time_ranges)
        )
        self.consignments += inst_consignments
        if instalacion.bahias is None or len(instalacion.bahias) == 0:
            msg = f"No hay bahias a procesar para esta instalación {instalacion}"
            self.update_info_status_report(msg, self.get_percentage(ix))
            return False, None, None
        installation_report = V2SRInstallationReportDetails(instalacion)
        return True, instalacion.bahias, installation_report

    def process(self):
        self.validate_entities()
        self.entity_time_ranges = self.get_entity_consignments()
        for ix, installation in enumerate(self.entity.instalaciones):
            try:
                success, bahia_list = self.create_installation_report_and_get_bahias_to_process(installation, ix)
                if not success:
                    continue

                for bahia in bahia_list:
                    bahia_time_ranges, bahia_consignments_list = (
                        get_date_time_ranges_from_consignment_time_ranges(bahia.get_document_id(), inst_time_ranges)
                    )
                    self.consignments += bahia_consignments_list
                    if bahia.tags is None or len(bahia.tags) == 0:
                        return False, f"No hay tags a procesar para bahia {bahia}", None
                    success, msg, df_tag_unavailability = processing_unavailability_of_tags(bahia.tags,
                                                                                            bahia_time_ranges,
                                                                                            self.pi_svr)
                    print(df_tag_unavailability)

            except Exception as e:
                error_log.error(f'Not able to process an installation: {str(e)}')
