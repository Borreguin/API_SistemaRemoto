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
import datetime as dt
from concurrent.futures import ProcessPoolExecutor
from typing import List

from app.common.PI_connection.PIServer.PIServerBase import PIServerBase
from app.common.util import get_time_in_minutes
from app.core.v2CalculationEngine.NodeStatusCalculation import NodeStatusCalculation as nodeStatus
from app.core.v2CalculationEngine.engine_util import generate_time_ranges
from app.core.v2CalculationEngine.util import create_report, get_pi_server, \
    delete_report_if_exists, verify_pi_server_connection, get_active_entities
from app.db.constants import attr_nombre, attr_tipo
from app.db.db_util import get_or_create_temporal_report, get_consignments
from app.db.v1.ProcessingState import TemporalProcessingStateReport
from app.db.v2.entities.v2_sRConsignment import V2SRConsignment
from app.db.v2.entities.v2_sREntity import V2SREntity
from app.db.v2.entities.v2_sRNode import V2SRNode
from app.db.v2.v2SRNodeReport.V2SRNodeDetailsPermanent import V2SRNodeDetailsPermanent
from app.db.v2.v2SRNodeReport.V2SRNodeDetailsTemporal import V2SRNodeDetailsTemporal
from app.main import db_connection


#
# print(f"PIServer Connection: {pi_svr.server}")
# report_ini_date = None
# report_end_date = None
# minutos_en_periodo = None
# sR_node_name = None
# n_lines = 40  # Para dar formato al log

# """ Time format """
# yyyy_mm_dd = "%Y-%m-%d"
# yyyy_mm_dd_hh_mm_ss = "%Y-%m-%d %H:%M:%S"


#     """
#     Procesa un nodo
#     :param force: Indica si debe forzar el guardado del reporte
#     :param nodo: v2SRNode
#     :param ini_date:  fecha inicial de reporte
#     :param end_date:  fecha final de reporte
#     :param save_in_db:  indica si se guardará en base de datos
#     :return: Success, NodeReport or None, (int msg, str msg)
#     """


class NodeExecutor:
    report_node: V2SRNodeDetailsTemporal | V2SRNodeDetailsPermanent = None
    status_node: TemporalProcessingStateReport = None
    pi_svr: PIServerBase = None
    node: V2SRNode = None
    report_id: str = None
    executor: ProcessPoolExecutor = None
    entities: List[V2SREntity] = None
    node_consignments: List[V2SRConsignment]

    def __init__(self, node: V2SRNode, report_id: str, ini_report_date: dt.datetime, end_report_date: dt.datetime,
                 save_in_db: bool, force: bool, permanent_report: bool):
        self.node = node
        self.report_id = report_id
        self.ini_report_date = ini_report_date
        self.end_report_date = end_report_date
        self.save_in_db = save_in_db
        self.force = force
        self.permanent_report = permanent_report
        self.success = False
        self.msg = None
        self.error_code = None
        self.minutes_in_period = None
        self.start_time = dt.datetime.now()
        self.check_connection()
        self.executor = ProcessPoolExecutor()

    def create_report(self):
        self.report_node = create_report(self.ini_report_date, self.end_report_date, self.node, self.permanent_report)

    def get_status_report(self):
        self.status_node = get_or_create_temporal_report(self.report_id)
        self.status_node.info[attr_nombre] = self.report_node.nombre
        self.status_node.info[attr_tipo] = self.report_node.tipo
        self.status_node.update_now()

    def check_connection(self):
        is_connected = db_connection()
        self.pi_svr = get_pi_server()
        if not is_connected or self.pi_svr is None:
            self.msg = "No se ha podido conectar a la base de datos: "
            self.msg += f"MongoBD" if not is_connected else f"PI Server"
            self.error_code = nodeStatus.NO_DATA_BASE_CONNECTION
            return False
        """ verificar si existe conexión con el servidor PI: """
        self.success, self.error_code = verify_pi_server_connection(self.pi_svr, self.status_node)
        return self.success

    def get_consignments(self):
        self.node_consignments = get_consignments(self.node.get_document_id(), self.ini_report_date, self.end_report_date)

    def get_entities(self):
        self.success, self.msg, self.entities = get_active_entities(self.node)

    def init(self):
        if self.error_code == nodeStatus.NO_DATA_BASE_CONNECTION:
            return
        self.minutes_in_period = get_time_in_minutes(self.ini_report_date, self.end_report_date)
        self.create_report()
        self.get_status_report()
        self.get_entities()
        """ Examinar si existe reporte asociado al nodo en este periodo de tiempo """
        #  si el reporte va ser re-escrito, entonces se elimina el existente de base de datos
        #  caso contrario no se puede continuar al ya existir un reporte asociado a este nodo
        self.success, node_report_db, self.msg = delete_report_if_exists(self.save_in_db, self.force, self.report_id,
                                                                         self.status_node, self.permanent_report)

    def processing_entities(self):
        for entity in self.entities:
            datetime_range = generate_time_ranges(self.node_consignments, self.ini_report_date, self.end_report_date)
            entity_consignments = get_consignments(entity.get_document_id(), self.ini_report_date, self.end_report_date)

    def processing_node(self):
        self.init()
        if not self.success:
            return
        self.processing_entities()

# def processing_node(nodo, ini_date: dt.datetime, end_date: dt.datetime, save_in_db=False, force=False):



#
#     """ Trabajando con cada entity_list, cada entity_list tiene utrs que tienen tags a calcular """


#     # Procesando cada utr dentro de un hilo:
#     structure, out_queue, n_threads = processing_each_utr_in_threads(entities, out_queue)
#
#     """ Recuperando los resultados de los informes provenientes de cada hilo """
#     report_node, status_node, in_memory_reports = collecting_report_from_threads(entities, out_queue, n_threads,
#                                                                                  report_node, status_node, structure)
#
#     """ Calculando tiempo de ejecución """
#     run_time = dt.datetime.now() - start_time
#     """ Calculando reporte en cada entidad """
#     [in_memory_reports[k].calculate() for k in in_memory_reports.keys()]
#     [in_memory_reports[k].reportes_utrs.sort(key=lambda x: x.disponibilidad_promedio_porcentage)
#      for k in in_memory_reports.keys()]
#     report_node.reportes_entidades = [in_memory_reports[k] for k in in_memory_reports.keys()]
#     report_node.reportes_entidades.sort(key=lambda x: x.disponibilidad_promedio_ponderada_porcentage)
#     report_node.entidades_fallidas = [r.entidad_nombre for r in report_node.reportes_entidades if r.numero_tags == 0]
#     report_node.calculate_all()
#     report_node.tiempo_calculo_segundos = run_time.total_seconds()
#     if report_node.numero_tags_total == 0:
#         status_node.failed()
#         status_node.msg = _6_no_existe_entidades[1]
#         status_node.update_now()
#         # guardar los resultados del nodo a pesar de no tener tags válidas
#         report_node.save(force=True)
#         return False, report_node, (_6_no_existe_entidades[0],
#                                     f"El nodo {sR_node_name} no contiene entidades válidas para procesar")
#     msg_save = (0, str())
#     try:
#         if force or save_in_db:
#             if reporte_ya_existe:
#                 msg_save = (_10_sobrescrito[0], "Reporte sobrescrito en base de datos")
#             else:
#                 msg_save = (_9_guardado[0], "Reporte escrito en base de datos")
#
#             print("reporte:", report_node.to_summary())
#             report_node.save(force=True)
#
#     except Exception as e:
#         status_node.failed()
#         status_node.msg = _7_no_es_posible_guardar[1]
#         status_node.update_now()
#         return False, report_node, (_7_no_es_posible_guardar[0],
#                                     f"No se ha podido guardar el reporte del nodo {sR_node_name} debido a: \n {str(e)}")
#
#     msg = f"{msg_save[1]}\nNodo [{sR_node_name}] procesado en: \t\t{run_time} \n" \
#           f"Numero de tags procesadas: \t{report_node.numero_tags_total}"
#     status_node.finished()
#     status_node.msg = msg_save[1]
#     status_node.info["run_time_seconds"] = run_time.total_seconds()
#     status_node.update_now()
#     return True, report_node, (msg_save[0], msg)
#
#

#
#
# def verify_pi_server_connection(status_node):
#     try:
#         pi_svr.server.Connect()
#         return True, (0, "Conexión exitosa con el servidor PI")
#     except Exception as e:
#         msg = f"No es posible la conexión con el servidor [{pi_svr.server.Name}] " \
#               f"\n [{str(e)}] \n [{traceback.format_exc()}]"
#         status_node.failed()
#         status_node.msg = _4_no_hay_conexion[1]
#         status_node.update_now()
#         return False, (_4_no_hay_conexion[0], msg)
#
#
# def get_active_entities(sR_node, status_node):
#     try:
#         entities = sR_node.entidades
#         entities = [e for e in entities if e.activado]
#
#         if len(entities) == 0:
#             status_node.failed()
#             status_node.msg = _6_no_existe_entidades[1]
#             status_node.update_now()
#             return False, None, (_6_no_existe_entidades[0],
#                                  f"No hay entidades a procesar en el nodo [{sR_node.nombre}]")
#
#         return True, entities, (0, "Entidades activas")
#     except Exception as e:
#         status_node.failed()
#         status_node.msg = _5_no_posible_entidades[1]
#         status_node.update_now()
#         return False, None, (_5_no_posible_entidades[0],
#                              "No se ha obtenido las entidades en el nodo [{sR_node.nombre}]: \n{str(e)}")
#
#
# def processing_each_utr_in_threads(entities, out_queue):
#     fault_utrs = list()
#     n_threads = 0
#     desc = f"Carg. UTRs [{sR_node_name}]"
#     desc = desc.ljust(n_lines)
#     structure = dict()  # to keep in memory the report structure
#     # processing each entity
#     for entity in tqdm(entities, desc=desc[:n_lines], ncols=100):
#
#         print(f"\nIniciando: [{entity.nombre}]") if verbosity else None
#
#         """ Seleccionar la lista de utrs a trabajar (activado: de la utr) """
#         UTRs = [utr for utr in entity.utrs if utr.activado]
#         for utr in UTRs:
#             # lista de tags a procesar y sus condiciones:
#             SR_Tags = utr.tags
#             lst_tags = [t.tag_name for t in SR_Tags if t.activado]
#             lst_conditions = [t.filter_expression for t in SR_Tags if t.activado]
#
#             th.Thread(name=utr.id_utr,
#                       target=processing_tags,
#                       kwargs={"utr": utr,
#                               "tag_list": lst_tags,
#                               "condition_list": lst_conditions,
#                               "q": out_queue}).start()
#             n_threads += 1
#             structure[utr.id_utr] = entity.entidad_nombre
#             # si no existe tags a procesar en esta UTR
#             if len(lst_tags) == 0:
#                 fault_utrs.append(utr.id_utr)
#
#     return structure, out_queue, n_threads
#
#
# def collecting_report_from_threads(entities, out_queue, n_threads, report_node, status_node, structure):
#     desc = f"Proc. UTRs [{sR_node_name}]".ljust(n_lines)  # descripción a presentar en barra de progreso
#     # Estructura para recopilar los reportes usando la estructura en memoria
#     in_memory_reports = dict()
#     for entity in entities:
#         report_entity = SREntityDetails(entidad_nombre=entity.entidad_nombre, entidad_tipo=entity.entidad_tipo,
#                                         periodo_evaluacion_minutos=minutos_en_periodo)
#         in_memory_reports[entity.entidad_nombre] = report_entity
#
#     for i in tqdm(range(n_threads), desc=desc[:n_lines], ncols=100):
#         success, utr_report, fault_tags, msg = out_queue.get()
#         report_node.tags_fallidas += fault_tags
#         """ Detalle por UTR """
#         if len(fault_tags) > 0:
#             report_node.tags_fallidas_detalle[str(utr_report.id_utr)] = fault_tags
#
#         # reportar en base de datos:
#         status_node.msg = f"Procesando nodo {sR_node_name}"
#         status_node.percentage = round(i / n_threads * 100, 2)
#         status_node.update_now()
#
#         # reportar tags no encontradas en log file
#         if len(fault_tags) > 0:
#             warn = f"\n[{sR_node_name}] [{utr_report.id_utr}] Tags no encontradas: \n" + '\n'.join(fault_tags)
#             log.warning(warn)
#
#         # verificando que los resultados sean correctos:
#         if not success or utr_report.numero_tags == 0:
#             report_node.utr_fallidas.append(utr_report.id_utr)
#             msg = f"\nNo se pudo procesar la UTR [{utr_report.id_utr}]: \n{msg}"
#             if utr_report.numero_tags == 0:
#                 msg += "No se encontro tags válidas"
#             log.error(msg)
#
#         # este reporte utr se guarda en su reporte de entidad correspondiente
#         entity_name = structure[utr_report.id_utr]
#         in_memory_reports[entity_name].reportes_utrs.append(utr_report)
#
#     return report_node, status_node, in_memory_reports
#
#
# def test():
#     """
#     Este test prueba:
#         - todos los nodos en Nodos:
#     :return:
#     """
#     global sR_node_name
#     global report_ini_date
#     global report_end_date
#
#     # mongo_config.update(dict(db="DB_DISP_EMS_TEST"))
#     connect(**mongo_config)
#
#     print("WARNING: Corriendo en modo DEBUG -- Este modo es solamente de prueba")
#     print(f">>> Procesando todos los nodos en DB [{mongo_config['db']}]")
#     # get date for last month:
#     # report_ini_date, report_end_date = u.get_dates_for_last_month()
#
#     all_nodes = SRNode.objects()
#     if len(all_nodes) == 0:
#         print("No hay nodos a procesar")
#         exit(-1)
#
#     all_nodes = [n for n in all_nodes if n.activado]
#     for node in all_nodes:
#         try:
#             print(f">>> Procesando el nodo: \n{node}")
#             for entidad in node.entidades:
#                 print(f"--- Entidad: \n{entidad}")
#             """
#                 for utr in entidad.utrs:
#                     # add consignments to test with:
#                     test_consignaciones = Consignments.objects(id_elemento=utr.utr_code).first()
#                     print(f"Insertando consignaciones ficticias para las pruebas")
#                     for c in range(2):
#                         n_days = r.uniform(1, 60)
#                         no_consignacion = "Test_consignacion" + str(r.randint(1, 1000))
#                         t_ini_consig = report_end_date - dt.timedelta(days=n_days)
#                         t_end_consig = t_ini_consig + dt.timedelta(days=r.uniform(0, 4))
#                         consignacion = Consignment(fecha_inicio=t_ini_consig, fecha_final=t_end_consig,
#                                                         no_consignacion=no_consignacion)
#                         # insertando la consignación
#                         print(test_consignaciones.insert_consignments(consignacion))
#                         # [print(c) for c in consignaciones.consignaciones]
#                     test_consignaciones.save()
#             """
#             # process this node:
#             print(f"\nProcesando el nodo: {node.nombre}")
#             success, NodeReport, msg = processing_node(node, report_ini_date, report_end_date, force=True)
#             if not success:
#                 log.error(msg)
#             else:
#                 print(msg)
#
#         except Exception as e:
#             msg = f"No se pudo procesar el nodo [{sR_node_name}] \n [{str(e)}]\n [{traceback.format_exc()}]"
#             log.error(msg)
#             print(msg)
#             continue
#     print("WARNING: Corriendo en modo DEBUG -- Este modo es solamente de prueba")
#     exit()
#
#
# if __name__ == "__main__":
#
#     # if debug:
#     #    report_ini_date = dt.datetime(2020, 8, 1)
#     #    report_end_date = dt.datetime(2020, 8, 2)
#     #    test()
#
#     # Configurando para obtener parámetros exteriores:
#     parser = argparse.ArgumentParser()
#
#     # Parámetro archivo que especifica la ubicación del archivo a procesar
#     parser.add_argument("nombre", help="indica el nombre del nodo a procesar",
#                         type=str)
#
#     # Parámetro fecha especifica la fecha de inicio
#     parser.add_argument("fecha_ini", help="fecha inicio, formato: YYYY-MM-DD H:M:S",
#                         type=u.valid_date_h_m_s)
#
#     # Parámetro fecha especifica la fecha de fin
#     parser.add_argument("fecha_fin", help="fecha fin, formato: YYYY-MM-DD H:M:S",
#                         type=u.valid_date_h_m_s)
#
#     # modo verbose para detección de errores
#     parser.add_argument("-v", "--verbosity", help="activar modo verbose",
#                         required=False, action="store_true")
#
#     # modo guardar en base de datos
#     parser.add_argument("-s", "--save", help="guardar resultado en base de datos",
#                         required=False, action="store_true")
#
#     # modo guardar en base de datos
#     parser.add_argument("-f", "--force", help="forzar guardar resultado en base de datos",
#                         required=False, action="store_true")
#
#     args = parser.parse_args()
#     print(args)
#     verbosity = args.verbosity
#     success, node_report, msg = None, None, ""
#
#     print("\n[{0}] \tProcesando información de [{1}] en el periodo: [{2}, {3}]"
#           .format(dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss), args.nombre, args.fecha_ini, args.fecha_fin))
#
#     try:
#         success, node_report, msg = processing_node(args.nombre, args.fecha_ini, args.fecha_fin, args.save, args.force)
#     except Exception as e:
#         tb = traceback.format_exc()
#         log.error(f"El nodo [{args.nombre}] no ha sido procesado de manera exitosa: \n {str(e)} \n{tb}")
#         exit(1)
#
#     if success:
#         if len(node_report.entidades_fallidas) > 0:
#             print("No se han procesado las siguientes entidades: \n{0}"
#                   .format("\n".join(node_report.entidades_fallidas)))
#         if len(node_report.tags_fallidas) > 0:
#             print("No se han procesado las siguientes tags: \n {0}".format("\n".join(node_report.tags_fallidas)))
#         print(f"\n[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] \t Proceso finalizado exitosamente:\n {msg}")
#         exit(int(msg[0]))
#     else:
#         to_print = f"\n[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] \t Proceso finalizado con problemas. " \
#                    f"\nRevise el archivo log en [/output] \n{msg}\n"
#         log.error(to_print)
#         exit(int(msg[0]))
