from typing import List
from app.common import report_node_detail_log as log
from app.common.PI_connection.PIServer.PIServerBase import PIServerBase
from app.common.PI_connection.pi_connect import create_pi_point
from app.common.PI_connection.pi_util import create_time_range
from app.core.v2CalculationEngine.DatetimeRange import DateTimeRange


def create_unavailability_condition(tag_name:str, condition:str):
    # creando la condición de indisponibilidad:
    if not "expr:" in condition:
        # 'tag1' = "condicion1" OR 'tag1' = "condicion2" OR etc. v1
        #  Compare(DigText('tag1'), "condicion1*") OR Compare(DigText('tag1'), "condicion2*")
        conditions = str(condition).split("#")
        # expression = f"'{tag}' = \"{conditions[0].strip()}\"" v1
        expression = f'Compare(DigText(\'{tag_name}\'), "{conditions[0].strip()}")'
        for c in conditions[1:]:
            # expression += f" OR '{tag}' = \"{c}\""
            expression += f' OR Compare(DigText(\'{tag_name}\'), "{c.strip()}")'
    else:
        expression = condition.replace("expr:", "")
    return expression

# time_ranges is a list of AFTimeRange
def get_tag_unavailability_from_history(tag_name:str, condition:str, time_ranges:List[DateTimeRange], pi_svr: PIServerBase) -> (bool, int, str):
    if len(time_ranges) == 0:
        return True, -1, "No hay periodos de tiempo a evaluar la indisponibilidad"

    try:
        # buscando la Tag en el servidor PI
        pt = create_pi_point(pi_svr, tag_name)
        if pt.pt is None:
            return False, 0, f"La tag {tag_name} no fue encontrada en el servidor PI"

        # creando la condición de indisponibilidad:
        expression = create_unavailability_condition(tag_name, condition)

        # Calculando el tiempo en el que se mantiene la condición de indisponibilidad
        #       tiempo_evaluacion:  [++++++++++++++++++++++++++++++++++++++++++++++]
        #       consignación:                  [-------------------]
        #       ocurrido:           [...::.....:::::::::::::::::::..:...:::........]
        #       minutos_disp:       [...::.....]                   [:...:::........]
        #       disponibilidad = #minutos_dispo/(tiempo_evaluacion - #minutos_consignados)
        #
        #                Ejemplo : tiempo mes:               30*60*24 =  43200 minutos
        #                          tiempo consignado:         2*24*60 =   2880 minutos
        #                          tiempo evaluar:       43200 - 2880 =  40320 minutos
        #                          porcentaje indisponible:           = t1 + t2 + tn  -> [(t_ind1 + t_ind2 + ...)/n]/40320
        # Reporte final:
        #                          tiempo disponible                        = 40300 minutos  (20 minutos indisponibilidad)
        #                          tiempo evaluar                           = 40320
        #                          tiempo consignado                        = 2880  minutos
        #                          tiempo procesado (t_operacion + t_consig)= 43200 minutos
        #

        indisponible_minutos = 0  # indisponibilidad acumulada
        # in minutes "mi"
        n_success = 0
        for time_range in time_ranges:
            af_time_range = create_time_range(time_range.start, time_range.end)
            value = pt.time_filter(af_time_range, expression, span=None, time_unit="mi")
            if value is None:
                continue
            # acumulando el tiempo de indisponibilidad
            indisponible_minutos += value[tag_name].iloc[0]
            n_success += 1
        return n_success > 0, indisponible_minutos, f'Tag {tag_name} was unavailable for {indisponible_minutos} minutes'

    except Exception as e:
        log.error(f"Error al momento de procesar la tag {tag_name}, detalles: \n{str(e)}")
        return False, 0, f"No fue posible obtener la indisponibilidad de la tag {tag_name}"
