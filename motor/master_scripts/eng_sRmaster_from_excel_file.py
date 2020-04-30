"""
Desarrollado en la Gerencia de Desarrollo Técnico
by: Roberto Sánchez Febrero 2020
motto:
"Whatever you do, work at it with all your heart, as working for the Lord, not for human master_scripts"
Colossians 3:23

1.	Nodo master:
•	Lee el archivo master.xlsx
•	Define las unidades de negocio a procesar y el directorio donde se encuentra el archivo referente
    a dicha unidad de negocio. Ejemplo: “Unidad Negocio 1 > node_scripts/unidad_de_negocio_1.xls”
•	Define el periodo a la que se ejecutará el cálculo de disponibilidad: inicio (“yyyy-mm-d1”) fin (“yyyy-mm-d2”)
•	Elimina/reescribe los archivos temporales dentro del directorio: “temp”
•	Crea nodos hijos mediante subprocesos distribuidos en los núcleos disponibles
•	Ejecuta el script
    “python eng_sRnode_from_excel_file.py unidad_negocio.xls yyyy-mm-d1 yyyy-mm-d2 -s”
•	Implementa un mecanismo que detecta la finalización de los nodos hijos
•	Guarda en base de datos los resultados, la base de datos se encuentra en “/output/disponibilidad.db”

"""

import argparse, datetime as dt
import io
import subprocess as sb
from my_lib import log_util as lu, utils as u
from my_lib.PI_connection import pi_connect as pi
import os, logging
import my_lib.sql3_db_manager.db_writer as db_

""" Variables globales"""
script_path = os.path.dirname(os.path.abspath(__file__))
master_path = os.path.join(script_path, "master_scripts")
log_path = os.path.join(script_path, "logs")
pi_svr = pi.PIserver()
time_range = None
span = None
sR_node_name = None

""" configuración de logger """
verbosity = False
debug = True
save_in_db = False
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
cl_period = db_.cl_period_ini
cl_n_minutos = db_.cl_n_minutos
cl_json = db_.cl_json
cl_empresa = db_.cl_empresa
cl_u_negocio = db_.cl_u_negocio
cl_disp_ponderada = "disp_ponderada"
cl_configuration_archive = db_.cl_configuration_archive

""" Nombre de hojas a utilizar """
h_main = "main"

""" Time format """
yyyy_mm_dd = "%Y-%m-%d"
yyyy_mm_dd_hh_mm_ss = "%Y-%m-%d %H:%M:%S"


def processing_master(_file_path: str, ini_date: dt.datetime, end_date: dt.datetime):
    global time_range
    global span
    global sR_node_name

    start_time = dt.datetime.now()
    """ Leyendo archivo Excel referente al master """
    file_name = _file_path.split("\\")[-1]
    dict_df, msg = u.read_excel(_file_path)
    if dict_df is None:
        return None, "El archivo master [{0}] no fue leído de manera correcta. \n" + msg
    if verbosity: print(msg)

    """ Crear time_range y span de acuerdo a fecha inicial y final en días """
    time_range, span, msg = u.create_datetime_and_span(ini_date, end_date)
    if time_range is None or span is None:
        return None, None, msg
    if verbosity: print(msg)

    """ Seleccionando los nodos a trabajar (columna: activado) """
    try:
        df_main = dict_df[h_main]
        df_main["activado"] = [str(x).lower() for x in df_main["activado"]]
        df_main[cl_configuration_archive] = [str(x).strip() for x in df_main[cl_configuration_archive]]
        df_main = df_main[df_main["activado"] == "x"]
    except Exception as e:
        return None, None, "La hoja [{0}] del archivo [{1}] no ha sido detectada correctamente. \n" \
            .format(h_main, file_name) + str(e)

    """ Procesando cada archivo de configuración por nodo"""
    nodes = list(df_main[cl_configuration_archive])
    child_processes = list()
    for node in nodes:
        print(f"[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] Empezando cálculo para: [{node}]")
        log_path = os.path.join(log_path, node.replace("xlsx", "log"))
        with io.open(log_path, mode='wb') as out:
            p = sb.Popen(
                ["python", "eng_sRnode_from_excel_file.py", node, ini_date.strftime(yyyy_mm_dd), end_date.strftime(yyyy_mm_dd), "--s"],
                stdout=out, stderr=out)
        # empezando un proceso a la vez
        child_processes.append(p)  # start this one, and immediately return to start another

    print(f"\n[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] Procesando todos los nodos...")
    # now you can join them together
    for cp in child_processes:
        cp.wait()

    print(f"[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] Finalizando todos los nodos, imprimiendo resultados")

    for node in nodes:
        try:
            log_path = os.path.join(log_path, node.replace("xlsx", "log"))
            f = open(log_path, "r", encoding="utf8")
            print(f.read())
        except Exception as e:
            print(f"[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] "
                  f"\t Problemas al procesar nodo [{node.replace('xlsx', '')}] \n" + str(e))

    run_time = dt.datetime.now() - start_time
    d_time = "{0}:{1}:{2}.{3}".format(int(run_time.seconds / 3600),
                                      int(run_time.seconds / 60),
                                      run_time.seconds,
                                      run_time.microseconds)

    print(f"[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] Cálculos finalizados \nTiempo de ejecución: [{d_time}]")
    return None, None, "end of script"


def test():
    global sR_node_name

    print("WARNING: Corriendo en modo DEBUG -- Este modo es solamente de prueba")
    file_name = "../masters/master.xlsx"
    file_path = os.path.join(master_path, file_name)

    # get date for yesterday:
    date_ini = dt.datetime.now() - dt.timedelta(days=1)
    date_end = dt.datetime.now()

    # get date for month:
    d_n = dt.datetime.now()
    date_ini = dt.datetime(year=d_n.year, month=d_n.month, day=1)
    date_end = dt.datetime(year=d_n.year, month=d_n.month, day=d_n.day)

    try:
        success, fault_entities, msg = processing_master(file_path, date_ini, date_end)
        print("WARNING: modo DEBUG activado: \n " + msg)
    except Exception as e:
        print(e)
        lg.error("No se pudo procesar el archivo master: [{0}] \n".format(file_path) + str(e))
    exit()


if __name__ == "__main__":

    if debug:
        test()

    # Configurando para obtener parámetros exteriores:
    parser = argparse.ArgumentParser()

    # Parámetro archivo que especifica la ubicación del archivo a procesar
    parser.add_argument("archivo", help="indica el path del archivo master a leer (master_scripts path)",
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
    file_path = os.path.join(master_path, args.archivo)
    print("\nProcesando información de [{0}]".format(file_path))
    try:
        success, fault_entities, msg = processing_master(file_path, args.fecha_ini, args.fecha_fin)
    except Exception as e:
        lg.error("El master [{0}] no ha sido procesado de manera exitosa".format(args.archivo))

    if success:
        if len(fault_entities) > 0:
            print("No se han procesado los siguientes nodos dentro del master: \n {0}".format(
                "\n".join(fault_entities)))
        print(msg)
        exit(0)
    else:
        print(msg)
        exit(-1)
