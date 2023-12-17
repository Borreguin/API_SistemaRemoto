import queue
import traceback
from typing import List

import pandas as pd

from app.common import report_node_detail_log as log
from app.common.PI_connection.PIServer.PIServerBase import PIServerBase
from app.core.v2CalculationEngine.DatetimeRange import DateTimeRange, get_total_time_in_minutes
from app.core.v2CalculationEngine.constants import columns_unavailability, cl_success, cl_processed_time
from app.core.v2CalculationEngine.node.tag_util import get_tag_unavailability_from_history
from app.db.v2.entities.v2_sRTag import V2SRTag


# # obtener consignaciones en el periodo de tiempo para generar periodos de consulta
#         # se debe exceptuar periodos de consignación
#
#         # verificar que exista el contenedor de consignaciones:
#         consDB = Consignments.objects(id_elemento=utr.utr_code).first()
#         if consDB is not None:
#             consignaciones_utr = consDB.consignments_in_time_range(report_ini_date, report_end_date)
#         else:
#             consignaciones_utr = []
#
#         # print("\n>>>>", utr.utr_nombre, consignaciones_utr)

# adjuntar consignaciones tomadas en cuenta:
# utr_report.consignaciones_detalle = consignaciones_utr


# tag_report = SRTagDetails(tag_name=tag, indisponible_minutos=indisponible_minutos)
# utr_report.indisponibilidad_detalle.append(tag_report)


# time_ranges is a list of AFTimeRange


# # la UTR no tiene tags válidas para el cálculo:
#         if len(utr_report.indisponibilidad_detalle) == 0:
#             utr_report.calculate(report_ini_date, report_end_date)
#             if q is not None:
#                 q.put((False, utr_report, fault_tags, f"La UTR {utr.utr_nombre} no tiene tags válidas"))
#             return False, utr_report, fault_tags, f"La UTR {utr.utr_nombre} no tiene tags válidas"
#
#         # All is OK until here:
#         utr_report.calculate(report_ini_date, report_end_date)
#         if q is not None:
#             q.put((True, utr_report, fault_tags, msg))
#         return True, utr_report, fault_tags, msg

def processing_unavailability_of_tags(tag_list: List[V2SRTag], time_ranges: List[DateTimeRange], pi_svr: PIServerBase):

    df_tag_unavailability = pd.DataFrame(columns=columns_unavailability, index=[tag.tag_name for tag in tag_list])
    df_tag_unavailability[cl_success] = False
    processed_time = get_total_time_in_minutes(time_ranges)
    try:
        log.info(f"processing_tags started with [{len(tag_list)}] tags")
        log.info(f"processing_tags started with [{len(time_ranges)}] time_ranges")

        # obtener la indisponibilidad de cada tag:
        for tag in tag_list:
            success, unavailability_minutes, msg = get_tag_unavailability_from_history(tag.tag_name, tag.filter_expression, time_ranges, pi_svr)
            df_tag_unavailability.loc[tag.tag_name] = [success, unavailability_minutes, processed_time, msg]
        return True, "processing_tags finished OK", df_tag_unavailability

    except Exception as e:
        tb = traceback.format_exc()
        msg = f"Error al momento de procesar las tags"
        log.error(f"{msg}, {e} \ndetalles: \n{tb}")
        return False, msg, df_tag_unavailability
