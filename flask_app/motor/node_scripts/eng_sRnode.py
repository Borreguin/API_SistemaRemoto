"""
Desarrollado en la Gerencia de Desarrollo Técnico
by: Roberto Sánchez Febrero 2020
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
import argparse, os, sys
import queue

script_path = os.path.dirname(os.path.abspath(__file__))
motor_path = os.path.dirname(script_path)
flask_path = os.path.dirname(motor_path)
project_path = os.path.dirname(flask_path)
sys.path.append(script_path)
sys.path.append(motor_path)
sys.path.append(project_path)

# import custom libraries:
from flask_app.dto.mongo_engine_handler.SRNodeReport.SRNodeReportTemporal import SRNodeDetailsTemporal
from flask_app.dto.mongo_engine_handler.SRNodeReport.sRNodeReportPermanente import SRNodeDetailsPermanente
from flask_app.motor import log_node

from flask_app.my_lib.PI_connection import pi_connect as pi
from flask_app.dto.mongo_engine_handler.ProcessingState import TemporalProcessingStateReport
import logging
import threading as th
from tqdm import tqdm
from random import randint

""" Import clases for MongoDB """
from flask_app.dto.mongo_engine_handler.SRNodeReport.sRNodeReportBase import *

mongo_config = init.MONGOCLIENT_SETTINGS
""" Variables globales"""
if not init.PRODUCTION_ENV or init.DEBUG:
    # pi server por defecto
    pi_svr = pi.PIserver()
else:
    # seleccionando cualquiera disponible
    idx = randint(0, len(init.PISERVERS) - 1)
    print(f"PI aleatorio seleccionado: {init.PISERVERS[int(idx)]}")
    PiServerName = init.PISERVERS[int(idx)]
    pi_svr = pi.PIserver(PiServerName)

print(f"PIServer Connection: {pi_svr.server}")
report_ini_date = None
report_end_date = None
minutos_en_periodo = None
sR_node_name = None
n_lines = 40  # Para dar formato al log

""" configuración de logger """
verbosity = False
debug = init.DEBUG
log = log_node
# nivel de eventos a registrar:
log.setLevel(logging.INFO)

""" Time format """
yyyy_mm_dd = "%Y-%m-%d"
yyyy_mm_dd_hh_mm_ss = "%Y-%m-%d %H:%M:%S"

# Tipos de errores:
_0_ok = (0, "Nodo procesado correctamente")
_1_inesperado = (1, "Error no determinado")
_2_no_existe = (2, "No se encontro el nodo")
_3_no_reconocido = (3, "Objeto nodo no reconocido")
_4_no_hay_conexion = (4, "No es posible la conexión con el servidor PI")
_5_no_posible_entidades = (5, "No se ha obtenido las entidades en el nodo")
_6_no_existe_entidades = (6, "No se pudo calcular las entidades, revise archivo Log")
_7_no_es_posible_guardar = (7, "No se ha podido guardar el reporte del nodo")
_8_reporte_existente = (8, "No ha sido calculado, el reporte ya existe en DB")
_9_guardado = (9, "Reporte guardado en base de datos")
_10_sobrescrito = (10, "Reporte sobrescrito en base de datos")
_11_sin_consignacion_cont = (11, "Las UTR no contienen contenedor de consignaciones")
eng_results = [_0_ok, _1_inesperado, _2_no_existe, _3_no_reconocido, _4_no_hay_conexion,
               _5_no_posible_entidades, _6_no_existe_entidades, _7_no_es_posible_guardar,
               _8_reporte_existente, _9_guardado, _10_sobrescrito, _11_sin_consignacion_cont]


def generate_time_ranges(consignaciones: list, ini_date: dt.datetime, end_date: dt.datetime):
    # La función encuentra el periodo en el cual se puede examinar la disponibilidad:
    if len(consignaciones) == 0:
        return [pi._time_range(ini_date, end_date)]

    # caso inicial:
    # * ++ tiempo de análisis ++*
    # [ periodo de consignación ]
    time_ranges = list()  # lleva la lista de periodos válidos
    tail = None  # inicialización
    end = end_date  # inicialización posible fecha final valida
    if consignaciones[0].fecha_inicio < ini_date:
        # [-----*++++]+++++++++++++++++++++*------
        tail = consignaciones[0].fecha_final  # lo que queda restante a analizar
        end = end_date  # por ser caso inicial se asume que se puede hacer el cálculo hasta el final del periodo

    elif consignaciones[0].fecha_inicio > ini_date and consignaciones[0].fecha_final < end_date:
        # --*++++[++++++]+++++++++*---------------
        start = ini_date  # fecha desde la que se empieza un periodo válido para calc. disponi
        # end = consignaciones[0].fecha_inicio  # fecha fin del periodo válido para calc. disponi
        tail = consignaciones[0].fecha_final  # siguiente probable periodo (lo que queda restante a analizar)
        time_ranges = [pi._time_range(start, consignaciones[0].fecha_inicio)]  # primer periodo válido

    elif consignaciones[0].fecha_inicio > ini_date and consignaciones[0].fecha_final >= end_date:
        # --*++++[+++++++++++++++*-----]----------
        # este caso es definitivo y no requiere continuar más alla:
        start = ini_date  # fecha desde la que se empieza un periodo válido para calc. disponi
        end = consignaciones[0].fecha_inicio  # fecha fin del periodo válido para calc. disponi
        return [pi._time_range(start, end)]

    elif consignaciones[0].fecha_inicio == ini_date and consignaciones[0].fecha_final < end_date:
        # --*[++++++++++]+++++++++*---------------
        start = consignaciones[0].fecha_final  # fecha desde la que se empieza un periodo válido para calc. disponi
        end = end_date  # fecha fin del periodo válido para calc. disponi
        tail = consignaciones[0].fecha_final  # siguiente probable periodo (lo que queda restante a analizar)
        time_ranges = []  # No hay periodo valido a evaluar

    elif consignaciones[0].fecha_inicio == ini_date and consignaciones[0].fecha_final == end_date:
        # ---*[+++++++]*---
        # este caso es definitivo y no requiere continuar más alla
        # nada que procesar en este caso
        return []

    # creando los demás rangos:
    for c in consignaciones[1:]:
        start = tail
        end = c.fecha_inicio
        if c.fecha_final < end_date:
            time_ranges.append(pi._time_range(start, end))
            tail = c.fecha_final
        else:
            end = c.fecha_inicio
            break
    # ultimo caso:
    time_ranges.append(pi._time_range(tail, end))
    return time_ranges


# La función devuelve el reporte por UTR
# busca las consignaciones existentes (si existe)
def processing_tags(utr: SRUTR, tag_list, condition_list, q: queue.Queue = None):
    global report_ini_date
    global report_end_date
    global minutos_en_periodo
    # reporte de entity_list
    utr_report = SRUTRDetails(id_utr=utr.id_utr, utr_nombre=utr.utr_nombre, utr_tipo=utr.utr_tipo,
                              periodo_evaluacion_minutos=minutos_en_periodo)

    try:

        fault_tags = list()  # tags que no fueron procesadas adecuadamente
        print(f"Procesando [{utr.utr_nombre}] con [{len(tag_list)}] tags") if verbosity else None

        # obtener consignaciones en el periodo de tiempo para generar periodos de consulta
        # se debe exceptuar periodos de consignación

        # verificar que exista el contenedor de consignaciones:
        consDB = Consignments.objects(id_elemento=utr.utr_code).first()
        if consDB is not None:
            consignaciones_utr = consDB.consignments_in_time_range(report_ini_date, report_end_date)
        else:
            consignaciones_utr = []

        # print("\n>>>>", utr.utr_nombre, consignaciones_utr)
        time_ranges = generate_time_ranges(consignaciones_utr, report_ini_date, report_end_date)

        # adjuntar consignaciones tomadas en cuenta:
        utr_report.consignaciones_detalle = consignaciones_utr

        # reportar por cada tag e incluir en el reporte utr
        msg = str()
        for tag, condition in zip(tag_list, condition_list):
            try:
                # buscando la Tag en el servidor PI
                pt = pi.PI_point(pi_svr, tag)
                if pt.pt is None:
                    fault_tags.append(tag)  # La tag no fue encontrada en el servidor PI
                    continue  # continue con la siguiente tag
                # creando la condición de indisponibilidad:
                if not "expr:" in condition:
                    # 'tag1' = "condicion1" OR 'tag1' = "condicion2" OR etc. v1
                    #  Compare(DigText('tag1'), "condicion1*") OR Compare(DigText('tag1'), "condicion2*")
                    conditions = str(condition).split("#")
                    # expression = f"'{tag}' = \"{conditions[0].strip()}\"" v1
                    expression = f'Compare(DigText(\'{tag}\'), "{conditions[0].strip()}")'
                    for c in conditions[1:]:
                        # expression += f" OR '{tag}' = \"{c}\""
                        expression += f' OR Compare(DigText(\'{tag}\'), "{c.strip()}")'
                else:
                    expression = condition.replace("expr:", "")

                # Calculando el tiempo en el que se mantiene la condición de indisponibilidad
                #       tiempo_evaluacion:  [++++++++++++++++++++++++++++++++++++++++++++++]
                #       consignación:                  [-------------------]
                #       ocurrido:           [...::.....:::::::::::::::::::..:...:::........]
                #       minutos_disp:       [...::.....]                  [.:...:::........]
                #       disponibilidad = #minutos_dispo/(tiempo_evaluacion - #minutos_consignados)
                #
                #                   UTR x: tiempo mes:               30*60*24 =  43200 minutos
                #                          tiempo consignado:         2*24*60 =   2880 minutos
                #                          tiempo evaluar:       43200 - 2880 =  40320 minutos
                #                          tiempo disponible(RS):             = t1 + t2   -> (t1+t2)/40320
                # Reporte final:
                #                          tiempo disponible                        = 40300 minutos  (20 minutos indispo)
                #                          tiempo evaluar                           = 40320
                #                          tiempo consignado                        = 2880  minutos
                #                          tiempo a evaluar (t_operacion + t_consig)= 43200 minutos
                #
                indisponible_minutos = 0  # indisponibilidad acumulada

                for time_range in time_ranges:
                    value = pt.time_filter(time_range, expression, span=None, time_unit="mi")
                    # acumulando el tiempo de indisponibilidad
                    indisponible_minutos += value[tag].iloc[0]
                tag_report = SRTagDetails(tag_name=tag, indisponible_minutos=indisponible_minutos)
                utr_report.indisponibilidad_detalle.append(tag_report)

            except Exception as e:
                fault_tags.append(tag)
                msg += f"[tag]: \n {str(e)} \n"
                print(msg) if verbosity else None

        # la UTR no tiene tags válidas para el cálculo:
        if len(utr_report.indisponibilidad_detalle) == 0:
            utr_report.calculate(report_ini_date, report_end_date)
            if q is not None:
                q.put((False, utr_report, fault_tags, f"La UTR {utr.utr_nombre} no tiene tags válidas"))
            return False, utr_report, fault_tags, f"La UTR {utr.utr_nombre} no tiene tags válidas"

        # All is OK until here:
        utr_report.calculate(report_ini_date, report_end_date)
        if q is not None:
            q.put((True, utr_report, fault_tags, msg))
        return True, utr_report, fault_tags, msg
    except Exception as e:
        tb = traceback.format_exc()
        if "dereference unknown document" in str(e):
            detalle = "El contenedor de consignaciones no ha sido aún creado, " \
                      "vuelva a crear el elemento a través del archivo Excel"
        else:
            detalle = "Error no determinado"
        msg = f"Error al momento de procesar las tags, observe detalles a continuación: \n{detalle}\n{str(e)} \n{tb}"

        if q is not None:
            utr_report.calculate(report_ini_date, report_end_date)
            q.put((False, utr_report, [], msg))
        return False, utr_report, [], msg


def processing_node(nodo, ini_date: dt.datetime, end_date: dt.datetime, save_in_db=False, force=False):
    """
    Procesa un nodo
    :param force: Indica si debe forzar el guardado del reporte
    :param nodo: [str, SRNode] Nombre del nodo, o su respectivo objeto SRNode
    :param ini_date:  fecha inicial de reporte
    :param end_date:  fecha final de reporte
    :param save_in_db:  indica si se guardará en base de datos
    :return: Success, NodeReport or None, (int msg, str msg)
    """
    global sR_node_name
    global report_ini_date
    global report_end_date
    global minutos_en_periodo
    reporte_ya_existe = False

    if isinstance(nodo, str):
        connect(**mongo_config)
        print(mongo_config)
        sR_node = SRNode.objects(nombre=nodo).first()
        if sR_node is None:
            return False, None, (_2_no_existe[0], f'No se encontro el nodo {nodo}')
    elif isinstance(nodo, SRNode):
        sR_node = nodo
    else:
        return False, None, (_3_no_reconocido[0], f'Objeto nodo no reconocido: {nodo}')

    """ Actualizar fecha y fin de reporte """
    sR_node_name = sR_node.nombre
    report_ini_date = ini_date
    report_end_date = end_date
    t_delta = report_end_date - report_ini_date
    minutos_en_periodo = t_delta.days * (60 * 24) + t_delta.seconds // 60 + (t_delta.seconds % 60)/60

    """ Creando reporte de nodo """
    if u.isTemporal(ini_date, end_date):
        report_node = SRNodeDetailsTemporal(nodo=sR_node, nombre=sR_node.nombre, tipo=sR_node.tipo,
                                            fecha_inicio=report_ini_date,
                                            fecha_final=report_end_date)
    else:
        report_node = SRNodeDetailsPermanente(nodo=sR_node, nombre=sR_node.nombre, tipo=sR_node.tipo,
                                              fecha_inicio=report_ini_date,
                                              fecha_final=report_end_date)
    status_node = TemporalProcessingStateReport(id_report=report_node.id_report,
                                                msg=f"Empezando cálculo del nodo: {sR_node_name}",
                                                finish=False, percentage=0)
    status_node.info["nombre"] = report_node.nombre
    status_node.info["tipo"] = report_node.tipo
    status_node.update_now()

    """ Examinar si existe reporte asociado al nodo en este periodo de tiempo """
    # si el reporte va ser re-escrito, entonces se elimina el existente de base de datos
    # caso contrario no se puede continuar al ya existir un reporte asociado a este nodo
    success, node_report_db, msg = delete_report_if_exists(save_in_db, force, report_node, status_node)
    if not success:
        # ya existe un reporte asociado, por lo que no se continúa
        return False, node_report_db, msg

    """ verificar si existe conexión con el servidor PI: """
    success, status = verify_pi_server_connection(status_node)
    if not success:
        # si no existe conexión con el servidor no se puede continuar:
        return False, node_report, status

    """ Seleccionando las entidades a trabajar (estado: activado) """
    """ entities es una lista de SREntity """
    success, entities, status = get_active_entities(sR_node, status_node)
    if not success:
        # si no hay entidades en el nodo, no se puede continuar
        return False, node_report, status

    """ Trabajando con cada entity_list, cada entity_list tiene utrs que tienen tags a calcular """
    # la cola que permitirá recibir los resultados de cada hilo
    out_queue = queue.Queue()
    # cronometrar el tiempo de cálculo
    start_time = dt.datetime.now()
    # Procesando cada utr dentro de un hilo:
    structure, out_queue, n_threads = processing_each_utr_in_threads(entities, out_queue)

    """ Recuperando los resultados de los informes provenientes de cada hilo """
    report_node, status_node, in_memory_reports = collecting_report_from_threads(entities, out_queue, n_threads,
                                                                                 report_node, status_node, structure)

    """ Calculando tiempo de ejecución """
    run_time = dt.datetime.now() - start_time
    """ Calculando reporte en cada entidad """
    [in_memory_reports[k].calculate() for k in in_memory_reports.keys()]
    [in_memory_reports[k].reportes_utrs.sort(key=lambda x: x.disponibilidad_promedio_porcentage)
     for k in in_memory_reports.keys()]
    report_node.reportes_entidades = [in_memory_reports[k] for k in in_memory_reports.keys()]
    report_node.reportes_entidades.sort(key=lambda x: x.disponibilidad_promedio_ponderada_porcentage)
    report_node.entidades_fallidas = [r.entidad_nombre for r in report_node.reportes_entidades if r.numero_tags == 0]
    report_node.calculate_all()
    report_node.tiempo_calculo_segundos = run_time.total_seconds()
    if report_node.numero_tags_total == 0:
        status_node.failed()
        status_node.msg = _6_no_existe_entidades[1]
        status_node.update_now()
        # guardar los resultados del nodo a pesar de no tener tags válidas
        report_node.save(force=True)
        return False, report_node, (_6_no_existe_entidades[0],
                                    f"El nodo {sR_node_name} no contiene entidades válidas para procesar")
    msg_save = (0, str())
    try:
        if force or save_in_db:
            if reporte_ya_existe:
                msg_save = (_10_sobrescrito[0], "Reporte sobrescrito en base de datos")
            else:
                msg_save = (_9_guardado[0], "Reporte escrito en base de datos")

            print("reporte:", report_node.to_summary())
            report_node.save(force=True)

    except Exception as e:
        status_node.failed()
        status_node.msg = _7_no_es_posible_guardar[1]
        status_node.update_now()
        return False, report_node, (_7_no_es_posible_guardar[0],
                                    f"No se ha podido guardar el reporte del nodo {sR_node_name} debido a: \n {str(e)}")

    msg = f"{msg_save[1]}\nNodo [{sR_node_name}] procesado en: \t\t{run_time} \n" \
          f"Numero de tags procesadas: \t{report_node.numero_tags_total}"
    status_node.finished()
    status_node.msg = msg_save[1]
    status_node.info["run_time_seconds"] = run_time.total_seconds()
    status_node.update_now()
    return True, report_node, (msg_save[0], msg)


def delete_report_if_exists(save_in_db, force, report_node, status_node):
    if save_in_db or force:
        """ Observar si existe el nodo en la base de datos """
        try:
            if u.isTemporal(report_node.fecha_inicio, report_node.fecha_final):
                node_report_db = SRNodeDetailsTemporal.objects(id_report=report_node.id_report).first()
            else:
                node_report_db = SRNodeDetailsPermanente.objects(id_report=report_node.id_report).first()
            reporte_ya_existe = (node_report_db is not None)
            """ Si se desea guardar y ya existe y no es sobreescritura, no se continúa """
            if reporte_ya_existe and save_in_db and not force:
                status_node.finished()
                status_node.msg = _8_reporte_existente[1]
                status_node.update_now()
                return False, node_report_db, _8_reporte_existente
            if reporte_ya_existe and force:
                node_report_db.delete()
                return True, None, "Reporte eliminado de manera correcta para reescritura"
            return True, None, "No es necesario eliminar el reporte"

        except Exception as e:
            msg = "Problema de concistencia en la base de datos"
            tb = traceback.format_exc()
            log.error(f"{msg} {str(e)} \n {tb}")
            return False, None, msg


def verify_pi_server_connection(status_node):
    try:
        pi_svr.server.Connect()
        return True, (0, "Conexión exitosa con el servidor PI")
    except Exception as e:
        msg = f"No es posible la conexión con el servidor [{pi_svr.server.Name}] " \
              f"\n [{str(e)}] \n [{traceback.format_exc()}]"
        status_node.failed()
        status_node.msg = _4_no_hay_conexion[1]
        status_node.update_now()
        return False, (_4_no_hay_conexion[0], msg)


def get_active_entities(sR_node, status_node):
    try:
        entities = sR_node.entidades
        entities = [e for e in entities if e.activado]

        if len(entities) == 0:
            status_node.failed()
            status_node.msg = _6_no_existe_entidades[1]
            status_node.update_now()
            return False, None, (_6_no_existe_entidades[0],
                                 f"No hay entidades a procesar en el nodo [{sR_node.nombre}]")

        return True, entities, (0, "Entidades activas")
    except Exception as e:
        status_node.failed()
        status_node.msg = _5_no_posible_entidades[1]
        status_node.update_now()
        return False, None, (_5_no_posible_entidades[0],
                             "No se ha obtenido las entidades en el nodo [{sR_node.nombre}]: \n{str(e)}")


def processing_each_utr_in_threads(entities, out_queue):
    fault_utrs = list()
    n_threads = 0
    desc = f"Carg. UTRs [{sR_node_name}]"
    desc = desc.ljust(n_lines)
    structure = dict()  # to keep in memory the report structure
    # processing each entity
    for entity in tqdm(entities, desc=desc[:n_lines], ncols=100):

        print(f"\nIniciando: [{entity.nombre}]") if verbosity else None

        """ Seleccionar la lista de utrs a trabajar (activado: de la utr) """
        UTRs = [utr for utr in entity.utrs if utr.activado]
        for utr in UTRs:
            # lista de tags a procesar y sus condiciones:
            SR_Tags = utr.tags
            lst_tags = [t.tag_name for t in SR_Tags if t.activado]
            lst_conditions = [t.filter_expression for t in SR_Tags if t.activado]

            th.Thread(name=utr.id_utr,
                      target=processing_tags,
                      kwargs={"utr": utr,
                              "tag_list": lst_tags,
                              "condition_list": lst_conditions,
                              "q": out_queue}).start()
            n_threads += 1
            structure[utr.id_utr] = entity.entidad_nombre
            # si no existe tags a procesar en esta UTR
            if len(lst_tags) == 0:
                fault_utrs.append(utr.id_utr)

    return structure, out_queue, n_threads


def collecting_report_from_threads(entities, out_queue, n_threads, report_node, status_node, structure):
    desc = f"Proc. UTRs [{sR_node_name}]".ljust(n_lines)  # descripción a presentar en barra de progreso
    # Estructura para recopilar los reportes usando la estructura en memoria
    in_memory_reports = dict()
    for entity in entities:
        report_entity = SREntityDetails(entidad_nombre=entity.entidad_nombre, entidad_tipo=entity.entidad_tipo,
                                        periodo_evaluacion_minutos=minutos_en_periodo)
        in_memory_reports[entity.entidad_nombre] = report_entity

    for i in tqdm(range(n_threads), desc=desc[:n_lines], ncols=100):
        success, utr_report, fault_tags, msg = out_queue.get()
        report_node.tags_fallidas += fault_tags
        """ Detalle por UTR """
        if len(fault_tags) > 0:
            report_node.tags_fallidas_detalle[str(utr_report.id_utr)] = fault_tags

        # reportar en base de datos:
        status_node.msg = f"Procesando nodo {sR_node_name}"
        status_node.percentage = round(i / n_threads * 100, 2)
        status_node.update_now()

        # reportar tags no encontradas en log file
        if len(fault_tags) > 0:
            warn = f"\n[{sR_node_name}] [{utr_report.id_utr}] Tags no encontradas: \n" + '\n'.join(fault_tags)
            log.warning(warn)

        # verificando que los resultados sean correctos:
        if not success or utr_report.numero_tags == 0:
            report_node.utr_fallidas.append(utr_report.id_utr)
            msg = f"\nNo se pudo procesar la UTR [{utr_report.id_utr}]: \n{msg}"
            if utr_report.numero_tags == 0:
                msg += "No se encontro tags válidas"
            log.error(msg)

        # este reporte utr se guarda en su reporte de entidad correspondiente
        entity_name = structure[utr_report.id_utr]
        in_memory_reports[entity_name].reportes_utrs.append(utr_report)

    return report_node, status_node, in_memory_reports


def test():
    """
    Este test prueba:
        - todos los nodos en Nodos:
    :return:
    """
    global sR_node_name
    global report_ini_date
    global report_end_date

    # mongo_config.update(dict(db="DB_DISP_EMS_TEST"))
    connect(**mongo_config)

    print("WARNING: Corriendo en modo DEBUG -- Este modo es solamente de prueba")
    print(f">>> Procesando todos los nodos en DB [{mongo_config['db']}]")
    # get date for last month:
    # report_ini_date, report_end_date = u.get_dates_for_last_month()

    all_nodes = SRNode.objects()
    if len(all_nodes) == 0:
        print("No hay nodos a procesar")
        exit(-1)

    all_nodes = [n for n in all_nodes if n.activado]
    for node in all_nodes:
        try:
            print(f">>> Procesando el nodo: \n{node}")
            for entidad in node.entidades:
                print(f"--- Entidad: \n{entidad}")
            """
                for utr in entidad.utrs:
                    # add consignments to test with:
                    test_consignaciones = Consignments.objects(id_elemento=utr.utr_code).first()
                    print(f"Insertando consignaciones ficticias para las pruebas")
                    for c in range(2):
                        n_days = r.uniform(1, 60)
                        no_consignacion = "Test_consignacion" + str(r.randint(1, 1000))
                        t_ini_consig = report_end_date - dt.timedelta(days=n_days)
                        t_end_consig = t_ini_consig + dt.timedelta(days=r.uniform(0, 4))
                        consignacion = Consignment(fecha_inicio=t_ini_consig, fecha_final=t_end_consig,
                                                        no_consignacion=no_consignacion)
                        # insertando la consignación
                        print(test_consignaciones.insert_consignments(consignacion))
                        # [print(c) for c in consignaciones.consignaciones]
                    test_consignaciones.save()
            """
            # process this node:
            print(f"\nProcesando el nodo: {node.nombre}")
            success, NodeReport, msg = processing_node(node, report_ini_date, report_end_date, force=True)
            if not success:
                log.error(msg)
            else:
                print(msg)

        except Exception as e:
            msg = f"No se pudo procesar el nodo [{sR_node_name}] \n [{str(e)}]\n [{traceback.format_exc()}]"
            log.error(msg)
            print(msg)
            continue
    print("WARNING: Corriendo en modo DEBUG -- Este modo es solamente de prueba")
    exit()


if __name__ == "__main__":

    # if debug:
    #    report_ini_date = dt.datetime(2020, 8, 1)
    #    report_end_date = dt.datetime(2020, 8, 2)
    #    test()

    # Configurando para obtener parámetros exteriores:
    parser = argparse.ArgumentParser()

    # Parámetro archivo que especifica la ubicación del archivo a procesar
    parser.add_argument("nombre", help="indica el nombre del nodo a procesar",
                        type=str)

    # Parámetro fecha especifica la fecha de inicio
    parser.add_argument("fecha_ini", help="fecha inicio, formato: YYYY-MM-DD H:M:S",
                        type=u.valid_date_h_m_s)

    # Parámetro fecha especifica la fecha de fin
    parser.add_argument("fecha_fin", help="fecha fin, formato: YYYY-MM-DD H:M:S",
                        type=u.valid_date_h_m_s)

    # modo verbose para detección de errores
    parser.add_argument("-v", "--verbosity", help="activar modo verbose",
                        required=False, action="store_true")

    # modo guardar en base de datos
    parser.add_argument("-s", "--save", help="guardar resultado en base de datos",
                        required=False, action="store_true")

    # modo guardar en base de datos
    parser.add_argument("-f", "--force", help="forzar guardar resultado en base de datos",
                        required=False, action="store_true")

    args = parser.parse_args()
    print(args)
    verbosity = args.verbosity
    success, node_report, msg = None, None, ""

    print("\n[{0}] \tProcesando información de [{1}] en el periodo: [{2}, {3}]"
          .format(dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss), args.nombre, args.fecha_ini, args.fecha_fin))

    try:
        success, node_report, msg = processing_node(args.nombre, args.fecha_ini, args.fecha_fin, args.save, args.force)
    except Exception as e:
        tb = traceback.format_exc()
        log.error(f"El nodo [{args.nombre}] no ha sido procesado de manera exitosa: \n {str(e)} \n{tb}")
        exit(1)

    if success:
        if len(node_report.entidades_fallidas) > 0:
            print("No se han procesado las siguientes entidades: \n{0}"
                  .format("\n".join(node_report.entidades_fallidas)))
        if len(node_report.tags_fallidas) > 0:
            print("No se han procesado las siguientes tags: \n {0}".format("\n".join(node_report.tags_fallidas)))
        print(f"\n[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] \t Proceso finalizado exitosamente:\n {msg}")
        exit(int(msg[0]))
    else:
        to_print = f"\n[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] \t Proceso finalizado con problemas. " \
                   f"\nRevise el archivo log en [/output] \n{msg}\n"
        log.error(to_print)
        exit(int(msg[0]))
