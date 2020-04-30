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


def run_all_nodes(report_ini_date: dt.datetime, report_end_date: dt.datetime):
    mongo_config = init.MONGOCLIENT_SETTINGS
    connect(**mongo_config)

    all_nodes = SRNode.objects()
    if len(all_nodes) == 0:
        msg = f"No hay nodos a procesar en db:[{mongo_config['db']}]"
        return False, None, msg
    try:
        child_processes = list()
        for node in all_nodes:

            # "Procesando cada nodo individual:
            log_path = os.path.join(output_path, f'{node.nombre}.log')
            with io.open(log_path, mode='wb') as out:
                p = sb.Popen(
                    ["python", node_script, node.nombre, report_ini_date.strftime(yyyy_mm_dd),
                     report_end_date.strftime(yyyy_mm_dd), "--s"],
                    stdout=out, stderr=out)
            # empezando un proceso a la vez
            child_processes.append(p)
            print(f"[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] Empezando el nodo [{node.nombre}]")

        # now you can join them together
        # uniendo todos los procesos juntos:
        print(f"\n[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] Procesando todos los nodos")
        for cp in child_processes:
            cp.wait()

        print(f"[{dt.datetime.now().strftime(yyyy_mm_dd_hh_mm_ss)}] Finalizando todos los nodos")

    except Exception as e:
        msg = f"No se pudo procesar todos los nodos \n [{str(e)}]\n [{traceback.format_exc()}]"
        lg.error(msg)
        print(msg)


def test():
    report_ini_date, report_end_date = u.get_dates_for_last_month()
    run_all_nodes(report_ini_date, report_end_date)


if __name__ == "__main__":

    if debug:
        test()