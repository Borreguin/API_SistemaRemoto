"""
Desarrollado en la Gerencia de Desarrollo Técnico
by: Roberto Sánchez Febrero 2020
motto:
"Whatever you do, work at it with all your heart, as working for the Lord, not for human master"
Colossians 3:23

2.	Nodo hijo:
•	Recibe parámetros enviados por el nodo master usando la librería argparse: (ubicación del archivo Excel, fecha de cálculo, etc).
•	Lee las configuraciones de nodo: correspondiente al nodo a ejecutar, y establece la fecha a realizar el cálculo.
•	Cada nodo contiene entidades (Subestaciones, centrales, etc). Cada entidad es procesada mediante un hilo del proceso nodo
•	Cada nodo es leído desde la base de datos MongoDB
•	Una vez ejecutado el nodo, genera un log de su ejecución
•	Guarda en base de datos los resultados, la base de datos se encuentra en “../../_db/mongo_db”

"""
import argparse
import datetime as dt
import queue
import traceback

from my_lib import utils as u
from my_lib.PI_connection import pi_connect as pi
from settings import initial_settings as init
import os, logging
import threading as th
from tqdm import tqdm

""" Import clases for MongoDB """
from my_lib.mongo_engine_handler.sRNodeReport import *
mongo_config = init.MONGOCLIENT_SETTINGS
""" Variables globales"""
script_path = os.path.dirname(os.path.abspath(__file__))
motor_path = os.path.dirname(script_path)
pi_svr = pi.PIserver()
report_ini_date = None
report_end_date = None
minutos_en_periodo = None
sR_node_name = None
n_lines = 40  # Para dar formato al log

""" configuración de logger """
verbosity = False
debug = init.FLASK_DEBUG
lg = init.LogDefaultConfig("Report.log").logger
# nivel de eventos a registrar:
lg.setLevel(logging.WARNING)

""" Time format """
yyyy_mm_dd = "%Y-%m-%d"
yyyy_mm_dd_hh_mm_ss = "%Y-%m-%d %H:%M:%S"


def generate_time_ranges(consignaciones: list, ini_date: dt.datetime, end_date: dt.datetime):

    if len(consignaciones) == 0:
        return [pi._time_range(ini_date, end_date)]

    # caso inicial:
    if consignaciones[0].fecha_inicio < ini_date:
        # [-----*++++]+++++++++++++++++++++*------
        tail = consignaciones[0].fecha_final
        end = end_date
        time_ranges = list()
    else:
        # --*++++[++++++]+++++++++*---------------
        start = ini_date
        end = consignaciones[0].fecha_inicio
        tail = consignaciones[0].fecha_final
        time_ranges = [pi._time_range(start, end)]

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


def processing_tags(entity: SREntity, tag_list, condition_list, q: queue.Queue = None):
    global report_ini_date
    global report_end_date
    global minutos_en_periodo

    # reporte de entidad
    entity_report = SREntityDetails(id_entidad=entity.id_entidad, nombre=entity.nombre, tipo=entity.tipo,
                                    periodo_evaluacion_minutos=minutos_en_periodo)

    fault_tags = list()  # tags que no fueron procesadas adecuadamente
    print(f"Procesando [{entity}] con [{len(tag_list)}] tags") if verbosity else None

    # obtener consignaciones en el periodo de tiempo para generar periodos de consulta
    # se debe exceptuar periodos de consignación
    consDB = Consignments.objects(id=entity.consignaciones.id).first()
    consignaciones_entidad = consDB.consignments_in_time_range(report_ini_date, report_end_date)
    time_ranges = generate_time_ranges(consignaciones_entidad, report_ini_date, report_end_date)

    # adjuntar consignaciones tomadas en cuenta:
    entity_report.consignaciones_detalle = consignaciones_entidad

    # reporte de tags
    tags_report = list()
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
                # 'tag1' = "condicion1" OR 'tag1' = "condicion2" OR etc.
                conditions = str(condition).split("#")
                expression = f"'{tag}' = \"{conditions[0].strip()}\""
                for c in conditions[1:]:
                    expression += f" OR '{tag}' = \"{c}\""
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
            tags_report.append(tag_report)

        except Exception as e:
            fault_tags.append(tag)
            msg += f"[tag]: {str(e)} \n"
            print(msg) if verbosity else None

    entity_report.indisponibilidad_detalle = tags_report
    entity_report.calculate()
    if q is not None:
        q.put((True, entity_report, fault_tags, msg))
    return True, entity_report, fault_tags, msg


def processing_node(nodo, ini_date: dt.datetime, end_date: dt.datetime, save_in_db=False):
    """
    Procesa un nodo
    :param nodo: [str, SRNode] Nombre del nodo, o su respectivo objeto SRNode
    :param ini_date:  fecha inicial de reporte
    :param end_date:  fecha final de reporte
    :param save_in_db:  indica si se guardará en base de datos
    :return:
    """
    global sR_node_name
    global report_ini_date
    global report_end_date
    global minutos_en_periodo

    if isinstance(nodo, str):
        connect(**mongo_config)
        sR_node = SRNode.objects(nombre=nodo).first()
        if sR_node is None:
            return False, None, f'No se encontro el nodo {nodo}'
    elif isinstance(nodo, SRNode):
        sR_node = nodo
    else:
        return False, None, f'Objeto nodo no reconocido: {nodo}'

    """ Actualizar fecha y fin de reporte """
    sR_node_name = sR_node.nombre
    report_ini_date = ini_date
    report_end_date = end_date
    t_delta = report_end_date - report_ini_date
    minutos_en_periodo = t_delta.days * (60 * 24) + t_delta.seconds // 60 + t_delta.seconds % 60

    """ Creando reporte de nodo """
    node_report = SRNodeDetails(nodo=sR_node, fecha_inicio=report_ini_date, fecha_final=report_end_date)

    """ verificar si existe conexión con el servidor PI: """
    try:
        pi_svr.server.Connect()
    except Exception as e:
        msg = f"No es posible la conexión con el servidor [{pi_svr.server.Name}] " \
              f"\n [{str(e)}] \n [{traceback.format_exc()}]"
        return False, None, msg

    """ Seleccionando las entidades a trabajar (estado: activado) """
    """ entities es una lista de SREntity """
    try:
        entities = sR_node.entidades
        entities = [e for e in entities if e.activado]
    except Exception as e:
        return False, None, f"No se ha obtenido las entidades en el nodo [{sR_node.nombre}]: \n{str(e)}"

    if len(entities) == 0:
        return False, None, f"No hay entidades a procesar en el nodo [{sR_node.nombre}]"

    """ Trabajando con cada entidad, cada entidad tiene tags a calcular """
    out_q = queue.Queue()
    fault_entities = list()
    start_time = dt.datetime.now()
    n_threads = 0
    desc = f"Carg. entidades [{sR_node_name}]"
    desc = desc.ljust(n_lines)
    for entity in tqdm(entities, desc=desc[:n_lines], ncols=100):

        print(f"\nIniciando: [{entity.nombre}]") if verbosity else None

        """ Seleccionar la lista de tags a trabajar (activado: de la tag) """

        # lista de tags a procesar y sus condiciones:
        SR_Tags = entity.tags
        lst_tags = [t.tag_name for t in SR_Tags if t.activado]
        lst_conditions = [t.filter_expression for t in SR_Tags if t.activado]

        # si existe una lista entonces empezar hilo para procesar
        if len(lst_tags) > 0:
            th.Thread(name=entity.id_entidad,
                      target=processing_tags,
                      kwargs={"entity": entity,
                              "tag_list": lst_tags,
                              "condition_list": lst_conditions,
                              "q": out_q}).start()
            n_threads += 1
        else:
            fault_entities.append(entity.nombre)

    desc = f"Proc. entidades [{sR_node_name}]".ljust(n_lines)  # descripción a presentar en barra de progreso

    """ Recuperando los resultados de los informes """
    reportes_entidades = list()
    tags_fallidas = list()
    for i in tqdm(range(n_threads), desc=desc[:n_lines], ncols=100):
        success, report_entidad, fault_tags, msg = out_q.get()

        # Añadiendo resultados:
        if len(fault_tags) > 0:
            warn = f"\n[{sR_node_name}] [{report_entidad.nombre}] Tags no encontradas: \n" + '\n'.join(fault_tags)
            lg.warning(warn)

        # verificando que los resultados sean correctos:
        if not success or report_entidad.numero_tags == 0:
            fault_entities.append(report_entidad.nombre)
            msg = f"\nNo se pudo procesar la entidad [{report_entidad.nombre}]: \n{msg}"
            print(msg) if verbosity else None
            lg.error(msg)
            continue

        # asignando reportes de entidad al informe general
        reportes_entidades.append(report_entidad)
        tags_fallidas = tags_fallidas + fault_tags

    """ Calculando tiempo de ejecución """
    run_time = dt.datetime.now() - start_time
    node_report.reportes_entidades = reportes_entidades
    node_report.tags_fallidas = tags_fallidas
    node_report.entidades_fallidas = fault_entities
    node_report.calculate_all()
    node_report.tiempo_calculo_segundos = run_time.total_seconds()
    if len(node_report.reportes_entidades) == 0:
        return False, node_report, f"El nodo {sR_node_name} no contiene entidades válidas para procesar"

    if save_in_db:
        try:
            node_report.save()
        except Exception as e:
            return False, fault_entities, \
                   f"No se ha podido guardar el reporte del nodo {sR_node_name} debido a: \n {str(e)}"

    msg = f"\nNodo [{sR_node_name}] procesado en: \t\t{run_time} \n" \
          f"Numero de tags procesadas: \t{node_report.numero_tags_total}"
    return True, node_report, msg


def test():
    import random as r
    global sR_node_name
    global report_ini_date
    global report_end_date

    mongo_config.update(dict(db="DB_DISP_EMS_TEST"))
    connect(**mongo_config)

    print("WARNING: Corriendo en modo DEBUG -- Este modo es solamente de prueba")
    print(f">>> Procesando todos los nodos en DB [{mongo_config['db']}]")
    # get date for last month:
    report_ini_date, report_end_date = u.get_dates_for_last_month()

    all_nodes = SRNode.objects()
    if len(all_nodes) == 0:
        print("No hay nodos a procesar")
        exit(-1)

    for node in all_nodes:
        try:
            print(f">>> Procesando el nodo: \n{node}")
            for entidad in node.entidades:
                print(f"--- Entidad: \n{entidad}")
                # add consignments to test with:
                test_consignaciones = Consignments.objects(id_entidad=entidad.id_entidad).first()
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
            # process this node:
            print(f"\nProcesando el nodo: {node.nombre}")
            success, fault_entities, msg = processing_node(node, report_ini_date, report_end_date)
            if not success:
                print(msg)
                lg.error(msg)
                continue
            print(msg)
        except Exception as e:
            msg = f"No se pudo procesar el nodo [{sR_node_name}] \n [{str(e)}]\n [{traceback.format_exc()}]"
            lg.error(msg)
            print(msg)
            continue
    print("WARNING: Corriendo en modo DEBUG -- Este modo es solamente de prueba")
    exit()


if __name__ == "__main__":

    #if debug:
    #    test()

    # Configurando para obtener parámetros exteriores:
    parser = argparse.ArgumentParser()

    # Parámetro archivo que especifica la ubicación del archivo a procesar
    parser.add_argument("nombre", help="indica el nombre del nodo a procesar",
                        type=str)

    # Parámetro fecha especifica la fecha de inicio
    parser.add_argument("fecha_ini", help="fecha inicio, formato: YYYY-MM-DD",
                        type=u.valid_date)

    # Parámetro fecha especifica la fecha de fin
    parser.add_argument("fecha_fin", help="fecha fin, formato: YYYY-MM-DD",
                        type=u.valid_date)

    # modo verbose para detección de errores
    parser.add_argument("-v", "--verbosity", help="activar modo verbose",
                        required=False, action="store_true")

    # modo guardar en base de datos
    parser.add_argument("-s", "--save", help="guardar resultado en base de datos",
                        required=False, action="store_true")

    args = parser.parse_args()
    verbosity = args.verbosity
    save_in_db = args.save
    lg.name = "node: " + args.nombre
    success, node_report, msg = None, None, ""

    print("\n[{0}] \tProcesando información de [{1}] en el periodo: \n\t\t\t[{2}, {3}]"
          .format(dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss), args.nombre, args.fecha_ini, args.fecha_fin))

    try:

        success, node_report, msg = processing_node(args.nombre, args.fecha_ini, args.fecha_fin)
    except Exception as e:
        tb = traceback.format_exc()
        lg.error(f"El nodo [{args.nombre}] no ha sido procesado de manera exitosa: \n {str(e)} \n{tb}")

    if success:
        if len(node_report.entidades_fallidas) > 0:
            print("No se han procesado las siguientes entidades: \n{0}".format("\n".join(node_report.entidades_fallidas)))
        if len(node_report.tags_fallidas) > 0:
            print("No se han procesado las siguientes tags: \n {0}".format("\n".join(node_report.tags_fallidas)))
        print(f"\n[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] \t Proceso finalizado exitosamente" + msg)
        exit(0)
    else:
        msg = f"\n[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] \t Proceso finalizado con problemas. " \
              f"Revise el archivo log en [/logs] {msg} \n"
        lg.error(msg)
        print(msg)
        exit(-1)