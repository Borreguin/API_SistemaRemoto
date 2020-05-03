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

from my_lib import utils as u
import io, os
import subprocess as sb
import traceback
from settings import initial_settings as init
from motor.node_scripts.eng_sRnode import eng_results
# general variables
script_path = os.path.dirname(os.path.abspath(__file__))
motor_path = os.path.dirname(script_path)
output_path = os.path.join(motor_path, "output")
node_script = os.path.join(motor_path, 'node_scripts', 'eng_sRnode.py')
debug = init.FLASK_DEBUG

""" Import clases for MongoDB """
from my_lib.mongo_engine_handler.sRNodeReport import *

""" Time format """
yyyy_mm_dd = "%Y-%m-%d"
yyyy_mm_dd_hh_mm_ss = "%Y-%m-%d %H:%M:%S"
lg = init.LogDefaultConfig("Report.log").logger


def run_all_nodes(report_ini_date: dt.datetime, report_end_date: dt.datetime, save_in_db=False, force=False):
    mongo_config = init.MONGOCLIENT_SETTINGS
    connect(**mongo_config)
    """ Buscando los nodos con los que se va a trabajar """
    all_nodes = SRNode.objects()
    all_nodes = [n for n in all_nodes if n.activado]
    if len(all_nodes) == 0:
        msg = f"No hay nodos a procesar en db:[{mongo_config['db']}]"
        return False, None, msg
    name_list = [n.nombre for n in all_nodes]
    return run_node_list(name_list, report_ini_date, report_end_date, save_in_db, force)


def run_node_list(node_list_name: list, report_ini_date: dt.datetime, report_end_date: dt.datetime,
                  save_in_db=False, force=False):

    """ variables para llevar logs"""
    msg = list()
    results = dict()

    """ check si es una lista de strings """
    check = all(isinstance(l, str) for l in node_list_name)
    if not check:
        return False, results, "La lista 'node_list_name' no es una lista válida de nombre de nodoss"

    try:
        child_processes = list()
        for node in node_list_name:

            # "Procesando cada nodo individual:
            log_path = os.path.join(output_path, f'{node}.log')
            with io.open(log_path, mode='wb') as out:
                to_run = ["python", node_script, node, report_ini_date.strftime(yyyy_mm_dd),
                          report_end_date.strftime(yyyy_mm_dd)]
                if save_in_db:
                    to_run += ["--s"]
                if force:
                    to_run += ["--f"]
                p = sb.Popen(to_run, stdout=out, stderr=out)
            # empezando un proceso a la vez
            child_processes.append(p)
            to_print = f"->[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] Empezando el nodo [{node}]"
            print(to_print)
            msg.append(to_print)

        # now you can join them together
        # uniendo todos los procesos juntos:
        print(f"\n[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] Procesando todos los nodos:")
        for cp in child_processes:
            cp.wait()
            # para dar seguimiento a los resultados:
            to_print = f"<-[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] (#st) Finalizando el nodo [{cp.args[2]}]"
            if cp.returncode in [0, 9]:
                to_print = to_print.replace("#st", "OK   ")
            elif cp.returncode in [8, 10]:
                to_print = to_print.replace("#st", "WARN ")
            else:
                to_print = to_print.replace("#st","ERROR")
            # details para identificar como finalizo el cálculo
            details = [d[1] for d in eng_results if d[0] == cp.returncode]
            results.update({str(cp.args[2]): details})
            print(to_print)
            msg.append(to_print)
        to_print = f"[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] Finalizando todos los nodos"
        print(to_print)
        msg.append(to_print)
        return True, results, msg

    except Exception as e:
        msg = f"No se pudo procesar todos los nodos \n [{str(e)}]\n [{traceback.format_exc()}]"
        lg.error(msg)
        return False, None, (msg)


def test():
    report_ini_date, report_end_date = u.get_dates_for_last_month()
    run_all_nodes(report_ini_date, report_end_date, save_in_db=True)


if __name__ == "__main__":

    if debug:
        test()