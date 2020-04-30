"""
Desarrollado en la Gerencia de Desarrollo Técnico
by: Roberto Sánchez Febrero 2020
motto:
"Whatever you do, work at it with all your heart, as working for the Lord, not for human master"
Colossians 3:23

2.	Nodo hijo:
•	Recibe parámetros enviados por el nodo master usando la librería argparse: (ubicación del archivo Excel, fecha de cálculo, etc).
•	Lee el archivo de nodo: “node_scripts/unidad_de_negocio.xls” correspondiente al nodo a ejecutar, y establece la fecha a realizar el cálculo.
•	Las hojas de cálculo representan las subestaciones asociadas a cada unidad de negocio. Cada hoja es procesada mediante un hilo del proceso nodo hijo.
•	Cada hoja de cálculo del archivo de nodo es leída a fin de obtener los nombres de las tags (variables) a procesar.
•	Una vez ejecutado el nodo, genera un log de su ejecución
•	Guarda en base de datos los resultados, la base de datos se encuentra en “/output/disponibilidad.db”

"""


import argparse, datetime as dt
import queue
import time
import traceback

from my_lib import log_util as lu, utils as u
from my_lib.PI_connection import pi_connect as pi
import os, logging
import threading as th
from tqdm import tqdm
from pandas import DataFrame
import my_lib.sql3_db_manager.db_writer as db_

""" Variables globales"""
script_path = os.path.dirname(os.path.abspath(__file__))
motor_path = os.path.dirname(script_path)
node_path = os.path.join(motor_path, "nodes")
temp_path = os.path.join(motor_path, "temp")
pi_svr = pi.PIserver()
time_range = None           # intervalo de tiempo en el que se desea calcular la disponibilidad  (2020-01-02 a 2020-01-02)
span = None                 # periodo en el que se desea reportar la disponibilidad Ex: (1 día, 20 días, etc)
sR_node_name = None
n_lines = 40                # Para dar formato al log
minutos_dia = 1440          # Constante que indica el número de minutos en el día

""" configuración de logger """
verbosity = False
debug = True
save_in_db = True
lg = logging.getLogger("DEBUG")
lg.addHandler(lu.SQLiteHandler())
# nivel de eventos a registrar:
lg.setLevel(logging.WARNING)

""" columnas de archivo Excel"""
cl_activado = db_.cl_activado
cl_entidades = db_.cl_entidades
cl_tag_name = db_.cl_tag_name
cl_f_expression = db_.cl_f_expression
cl_indisp_acc = db_.cl_indisp_acc
cl_disp_avg = db_.cl_disp_avg
cl_perc_disp = db_.cl_perc_disp
cl_utr = db_.cl_utr
cl_tags_problema = db_.cl_tags_problema
cl_n_tags = db_.cl_n_tags
cl_name = db_.cl_name
cl_weight = db_.cl_weight
cl_period_ini = db_.cl_period_ini
cl_period_end = db_.cl_period_end
cl_n_minutos = db_.cl_n_minutos
cl_json = db_.cl_json
cl_empresa = db_.cl_empresa
cl_u_negocio = db_.cl_u_negocio
cl_disp_ponderada = db_.cl_disp_ponderada
cl_configuration_archive = db_.cl_configuration_archive

""" Nombre de hojas a utilizar """
h_main = "main"
h_tags = "tags"

""" Time format """
yyyy_mm_dd = "%Y-%m-%d"
yyyy_mm_dd_hh_mm_ss = "%Y-%m-%d %H:%M:%S"


def saving_details(df_details, n_times=0):
    if n_times > 3:
        return False, "Se ha intentado guardar los detalles más de 3 veces"

    try:
        db = db_.DBWriterDisponibilidad()
        if cl_json not in df_details.columns:
            df_details[cl_json] = ""
        return db.insert_or_replace_details(df_details)

    except Exception as e:
        time.sleep(0.5)
        success, msg = saving_details(df_details, n_times+1)
        return success, msg + f"\n{str(e)} \n {traceback.format_exc()}"


def saving_summary(df_details, n_times=0):
    if n_times > 3:
        return False, "Se ha intentado guardar el resumen más de 3 veces"

    try:
        df_summary = DataFrame(columns=[cl_period_ini, cl_period_end, cl_n_minutos, cl_configuration_archive,
                                        cl_n_tags, cl_disp_ponderada, cl_json], index=[sR_node_name])

        # Disponibilidad ponderada usando la ponderación y la disponibilidad promedio:
        df_details[cl_disp_ponderada] = df_details[cl_weight]*df_details[cl_disp_avg]

        # Configurando el DataFrame de resumen:
        df_summary.loc[[sR_node_name], [cl_period_ini]] = df_details[cl_period_ini].iloc[0]
        df_summary.loc[[sR_node_name], [cl_period_end]] = df_details[cl_period_end].iloc[0]
        df_summary.loc[[sR_node_name], [cl_n_minutos]] = df_details[cl_n_minutos].iloc[0]
        df_summary.loc[[sR_node_name], [cl_configuration_archive]] = sR_node_name

        # Totalizado de numero de señales a tener en cuenta:
        df_summary.loc[[sR_node_name], [cl_n_tags]] = df_details[cl_n_tags].sum()

        # Disponibilidad del nodo procesado:
        # La sumatoria de la disponibilidad ponderada dividido para el número de minutos en el periodo evaluado:
        df_summary.loc[[sR_node_name], [cl_perc_disp]] = df_details[cl_disp_ponderada].sum() / df_details[cl_n_minutos].iloc[0]

        db = db_.DBWriterDisponibilidad()
        return db.insert_or_replace_summary(df_summary)
    except Exception as e:
        time.sleep(0.5)
        success, msg = saving_summary(df_details, n_times+1)
        return success, msg + f"\n{str(e)} \n {traceback.format_exc()}"


def processing_tags(entity, tag_list, condition_list, q: queue.Queue = None):
    global time_range
    global span
    acc_indispo = 0             # indisponibilidad acumulada
    n_tags = 0                  # número de tags procesadas
    fault_tags = list()         # tags que no fueron procesadas adecuadamente
    if verbosity: print("Procesando [{0}] con [{1}] tags".format(entity, len(tag_list)))

    for tag, condition in zip(tag_list, condition_list):
        try:
            # buscando la Tag en el servidor PI
            pt = pi.PI_point(pi_svr, tag)
            if pt.pt is None:
                fault_tags.append(tag)      # La tag no fue encontrada en el servidor PI
                continue                    # continue con la siguiente tag
            # creando la condición de indisponibilidad:
            # 'tag1' = "condicion1" OR 'tag1' = "condicion2" OR etc.
            conditions = str(condition).split("#")
            expression = "'{tag_name}' = \"{condition}\"".format(tag_name=tag, condition=conditions[0].strip())
            for c in conditions[1:]:
                expression += " OR '{tag_name}' = \"{condition}\"".format(tag_name=tag, condition=c.strip())

            # Calculando el tiempo en el que se mantiene la condición de indisponibilidad
            # TODO: tiempo_evaluacion:  [++++++++++++++++++++++++++++++++++++++++++++++]
            #       consignación:                  [-------------------]
            #       ocurrido:           [...::.....:::::::::::::::::::..:...:::........]
            #       minutos_disp:       [...::.....]                  [.:...:::........]
            #       disponibilidad = #minutos_dispo/(tiempo_evaluacion - #minutos_consignados)
            #
            #
            #
            # 13 y 14 consignados
            # 13  y 14 disp 100% equi min
            # mes min disp /30 dias (min)
            #
            #                   UTR x: tiempo mes:               30*60*24 =  43200 minutos
            #                          tiempo consignado:         2*24*60 =   2880 minutos
            #                          tiempo evaluar (RS):  43200 - 2880 =  40320 minutos
            #                          tiempo evaluar (CDH):              =  43200 minutos
            #                          tiempo disponible(RS):             = t1 + t2   -> (t1+t2)/40320
            #                          tiempo disponible(CDH):            = (43200 + 2880)/43200 > 1
            # Reporte final:
            #                          tiempo disponible                        = 40300 minutos  (20 minutos indispo)
            #                          tiempo evaluar                           = 40320
            #                          tiempo consignado                        = 2880  minutos
            #                          tiempo a evaluar (t_operacion + t_consig)= 43200 minutos
            value = pt.time_filter(time_range, expression, span, time_unit="mi")
            # acumulando el tiempo de indisponibilidad
            acc_indispo += value[tag].iloc[0]
            n_tags += 1
        except Exception as e:
            fault_tags.append(tag)
            msg = "La siguiente tag [{0}] presentó problemas de cálculo. \n".format(tag) + str(e)
            if verbosity: print(msg)

    if q is not None:
        q.put((True, entity, acc_indispo, n_tags, fault_tags))
    return True, entity, acc_indispo, n_tags, fault_tags


def processing_node(_file_path: str, ini_date: dt.datetime, end_date: dt.datetime):
    global time_range
    global span
    global sR_node_name

    """ Leyendo archivo Excel referente al nodo """
    file_name = _file_path.split("\\")[-1]
    dict_df, msg = u.read_excel(_file_path)
    if dict_df is None:
        return False, None, "El archivo [{0}] no fue leído de manera correcta. \n" + msg
    if verbosity: print(msg)

    """ Crear time_range y span de acuerdo a fecha inicial y final en días """
    time_range, span, msg = u.create_datetime_and_span(ini_date, end_date)
    if time_range is None or span is None:
        return False, None, msg
    if verbosity: print(msg)

    """ verificar si existe conexión con el servidor PI: """
    try:
        pi_svr.server.Connect()
    except Exception as e:
        msg = f"No es posible la conexión con el servidor [{pi_svr.server.Name}] " \
              f"\n [{str(e)}] \n [{traceback.format_exc()}]"
        return False, None, msg

    """ Seleccionando las entidades a trabajar (columna: activado) """
    try:
        df_main = dict_df[h_main]
        df_main["activado"] = [str(x).lower() for x in df_main["activado"]]
        df_main[cl_entidades] = [str(x).strip() for x in df_main[cl_entidades]]
        df_main = df_main[df_main["activado"] == "x"]
    except Exception as e:
        return False, None, "La hoja [{0}] del archivo [{1}] no ha sido detectada correctamente. \n"\
            .format(h_main, file_name) + str(e)

    """ Seleccionar la lista de tags a trabajar (columna: activado) """
    try:
        # Filtrando entidades a calcular datos:
        df_tags = dict_df[h_tags]
        df_tags["activado"] = [str(x).lower() for x in df_tags["activado"]]
        df_tags[cl_utr] = [str(x).strip() for x in df_tags[cl_utr]]
        df_tags = df_tags[df_tags["activado"] == "x"]
    except Exception as e:
        return False, None, "La hoja [{0}] del archivo [{1}] no ha sido detectada correctamente. \n"\
            .format(h_tags, file_name) + str(e)

    """ Definiendo entidades para trabajar """
    entities = list(df_main[cl_entidades])

    """ Trabajando con cada entidad, cada entidad tiene tags a calcular """
    out_q = queue.Queue()
    fault_entities = list()
    start_time = dt.datetime.now()
    n_threads = 0
    desc = f"Carg. entidades [{file_name}]"
    desc = desc.ljust(n_lines)
    for entity in tqdm(entities, desc=desc[:n_lines], ncols=100):
        if verbosity: print("\nIniciando: [{0}]".format(entity))

        # lista de tags a procesar y sus condiciones:
        lst_tags = list(df_tags[df_tags[cl_utr] == entity][cl_tag_name])
        lst_conditions = list(df_tags[df_tags[cl_utr] == entity][cl_f_expression])

        # si existe una lista entonces empezar hilo para procesar
        if len(lst_tags) > 0:
            th.Thread(name=entity,
                      target=processing_tags,
                      kwargs={"entity": entity,
                              "tag_list": lst_tags,
                              "condition_list": lst_conditions,
                              "q": out_q}).start()
            n_threads += 1
        else:
            fault_entities.append(entity)

    """ Recolectando información final desde los hilos """
    df_final = DataFrame(columns=[cl_name, cl_entidades, cl_indisp_acc, cl_n_tags,
                                  cl_disp_avg, cl_perc_disp, cl_weight, cl_period_ini, cl_period_end,
                                  cl_n_minutos, cl_tags_problema],
                         index=range(n_threads))

    """ Calculando número de minutos en el periodo establecido """
    diff_time = end_date - ini_date

    n_minutos = diff_time.days*minutos_dia + diff_time.seconds/60
    desc = f"Proc. entidades [{file_name}]".ljust(n_lines)              # descripción a presentar en barra de progreso

    for i in tqdm(range(n_threads), desc=desc[:n_lines], ncols=100):
        success, entity, indisp_acc, n_tags, fault_tags = out_q.get()

        # verificando que los resultados sean correctos:
        if not success or n_tags == 0:
            msg = f"\nNo se pudo procesar la entidad [{entity}] debido a: \n {fault_tags}"
            print(msg)
            lg.error(msg)
            continue

        # asignando nombre de la entidad
        name_entity = df_main[df_main[cl_entidades] == entity][cl_name].iloc[0]
        df_final[cl_name].loc[i] = name_entity

        # Añadiendo resultados:
        df_final.loc[[i], cl_entidades] = entity
        df_final.loc[[i], cl_n_tags] = int(n_tags)

        disponibilidad = round(n_minutos - indisp_acc/n_tags, 2)
        df_final.loc[[i], cl_indisp_acc] = indisp_acc
        df_final.loc[[i], cl_disp_avg] = disponibilidad
        df_final.loc[[i], cl_perc_disp] = round(disponibilidad/n_minutos*100, 2)

        if len(fault_tags) > 0:
            df_final.loc[[i], cl_tags_problema] = '\n'.join(fault_tags)
            lg.warning(f"[{file_name}] [{entity}] Tags no encontradas: \n" + '\n'.join(fault_tags))

    df_final[cl_period_ini] = ini_date.strftime(yyyy_mm_dd_hh_mm_ss)
    df_final[cl_period_end] = end_date.strftime(yyyy_mm_dd_hh_mm_ss)
    df_final[cl_n_minutos] = n_minutos
    if df_final[cl_n_tags].sum() == 0:
        return False, entities, f"No se ha podido procesar el archivo [{file_name}] debido a problemas de conexión"
    df_final[cl_weight] = df_final[cl_n_tags]/df_final[cl_n_tags].sum()
    df_final.sort_values(by=[cl_disp_avg], inplace=True)

    """ Calculando tiempo de ejecución """
    run_time = dt.datetime.now() - start_time
    d_time = "{0}:{1}:{2}.{3}".format(int(run_time.seconds / 3600),
                                      int(run_time.seconds / 60),
                                      run_time.seconds,
                                      run_time.microseconds)

    """ Guardando el resultado en el folder temp """
    try:
        final_file_path = os.path.join(temp_path, "_" + file_name)
        df_final.set_index(cl_entidades, inplace=True)
        df_final.to_excel(final_file_path)
    except Exception as e:
        lg.error("No se pudo guardar el archivo [{0}] \n".format(_file_path) + str(e))

    """ Imprimiendo novedades si existe: """
    if len(fault_entities) > 0:
        msg = "[{0}] Las siguientes entidades no han sido procesadas: ".format(file_name) \
              + "\n".join(fault_entities)
        print(msg)
        lg.warning(msg)

    """ Guardando resultado en base de datos """
    if save_in_db:
        df_final[cl_empresa] = file_name
        df_final[cl_entidades] = df_final.index
        success, msg_1 = saving_details(df_final)
        if not success:
            lg.error(msg_1)
        success, msg_2 = saving_summary(df_final)
        if not success:
            lg.error(msg_2)
        print("\n" + msg_1, "\n" + msg_2)


    msg = "\nProceso finalizado en: \t\t{0} \n" \
            "Numero de tags procesadas: \t{1}".format(d_time, df_final[cl_n_tags].sum())
    return True, fault_entities, msg


def test():
    global sR_node_name

    print("WARNING: Corriendo en modo DEBUG -- Este modo es solamente de prueba")
    file_name = "TRANSELECTRIC.xlsx"
    file_path = os.path.join(node_path, file_name)

    # get date for yesterday:
    date_ini = dt.datetime.now() - dt.timedelta(days=1)
    date_end = dt.datetime.now()

    # get date for month:
    d_n = dt.datetime.now()
    date_ini = dt.datetime(year=d_n.year, month=d_n.month, day=1)
    date_end = dt.datetime(year=d_n.year, month=d_n.month, day=d_n.day)

    try:
        success, fault_entities, msg = processing_node(file_path, date_ini, date_end)
        msg = "WARNING: modo DEBUG activado: \n " + msg
        if not success:
            lg.error(msg)
        print(msg)
        exit(-1)
    except Exception as e:
        msg = "No se pudo procesar el archivo [{0}] \n [{1}]\n [{2}]".format(file_path, str(e), traceback.format_exc())
        lg.error(msg)
        print(msg)
        exit(-1)
    exit()

# Example:
# python eng_sRnode_from_excel_file.py TRANSELECTRIC.xlsx 2020-01-01 2020-01-28


if __name__ == "__main__":

    if debug:
        test()

    # Configurando para obtener parámetros exteriores:
    parser = argparse.ArgumentParser()

    # Parámetro archivo que especifica la ubicación del archivo a procesar
    parser.add_argument("archivo", help="indica el path del archivo a leer",
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
    lg.name = "node: " + args.archivo
    success, fault_entities, msg = None, None, ""
    file_path = os.path.join(node_path, args.archivo)
    print("\n[{0}] \tProcesando información de [{1}] en el periodo: \n\t\t\t[{2}, {3}]"
          .format(dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss), args.archivo, args.fecha_ini, args.fecha_fin))
    try:
        success, fault_entities, msg = processing_node(file_path, args.fecha_ini, args.fecha_fin)
    except Exception as e:
        lg.error("El nodo [{0}] no ha sido procesado de manera exitosa".format(args.archivo))

    if success:
        if len(fault_entities) > 0:
            print("No se han procesado las siguientes entidades: \n {0}".format("\n".join(fault_entities)))
        print(f"\n[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] \t Proceso finalizado exitosamente" + msg)
        exit(0)
    else:
        msg = f"\n[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] \t Proceso finalizado con problemas. " \
              f"Revise el archivo log en [/logs]" + msg
        lg.error(msg)
        print(msg)
        exit(-1)
