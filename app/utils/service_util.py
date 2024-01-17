from app.db.constants import SR_REPORTE_SISTEMA_REMOTO, V1_SR_NODE_LABEL, V2_SR_NODE_LABEL
from app.db.db_util import get_final_report_v1_by_id, get_final_report_v2_by_id
from app.db.v2.v2SRNodeReport.report_util import get_final_report_id


def get_sr_final_report_by_version(ini_date: str, end_date: str, version: str = None, is_permanent: bool = False):
    report_id = get_final_report_id(SR_REPORTE_SISTEMA_REMOTO, ini_date, end_date)
    if version is None or version == V1_SR_NODE_LABEL:
        return get_final_report_v1_by_id(report_id, is_permanent)
    else:
        return get_final_report_v2_by_id(report_id, is_permanent)

def get_sr_final_report(ini_date: str, end_date: str, is_permanent: bool = False):
    final_report = get_sr_final_report_by_version(ini_date, end_date, V2_SR_NODE_LABEL, is_permanent)
    if final_report is not None:
        return final_report
    return get_sr_final_report_by_version(ini_date, end_date, V1_SR_NODE_LABEL, is_permanent)





