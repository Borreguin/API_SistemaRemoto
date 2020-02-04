import argparse, os
import datetime as dt
import pandas as pd
import pickle as pkl
from my_lib.PI_connection import pi_connect as pi

pi_svr = pi.PIserver()
script_path = os.path.dirname(os.path.abspath(__file__))

def valid_date(s):
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "El parámetro: '{0}' no es una fecha válida, (formato YYYY-MM-DD).".format(s)
        raise argparse.ArgumentTypeError(msg)


def read_excel(file_name):
    """
    Lee un archivo excel y devuelve un diccionario de DataFrames
    :param file_name: path del archivo a leer
    :return: diccionario de DataFrames
    """
    # variables generales usadas en el script:
    name = file_name.split('\\')
    name = name[-1]
    file_time = None
    last_time = -1
    df_update = pd.DataFrame(columns=["time"])

    # configurando rutas:
    db_path = script_path.replace("my_lib", "_db")
    pkl_file = os.path.join(db_path, name.replace("xlsx", "pkl"))
    json_file = os.path.join(db_path, "update_time.json")

    # verificando si los archivos existen:
    json_exists = os.path.exists(json_file)
    file_exists = os.path.exists(file_name)
    pkl_exists = os.path.exists(pkl_file)

    if file_exists:
        # obtener la hora de última modificación
        file_time = os.path.getmtime(file_name)
    else:
        msg = "El archivo {0} no existe".format(file_name)
        print(msg)
        return None, msg

    # si el archivo json existe, obtener last_time
    if json_exists:
        df_update = pd.read_json(json_file)
        if name in df_update.index:
            last_time = df_update["time"][name]

    # grabar file_time in df_update:
    df_update.loc[name] = [file_time]
    df_update.to_json(json_file)

    # si hay una modificación en el archivo Excel, leer el archivo
    # y transformarlo en formato pkl
    if last_time != file_time or not pkl_exists:
        try:
            xls = pd.ExcelFile(file_name)
            dt_excel = dict()
            for s in xls.sheet_names:
                dt_excel[s] = pd.read_excel(xls, s)
            with open(pkl_file, 'wb') as handle:
                pkl.dump(dt_excel, handle, protocol=pkl.HIGHEST_PROTOCOL)
            return dt_excel, "[{0}] Leído correctamente".format(file_name)
        except Exception as e:
            return None, str(e)
    elif pkl_exists and last_time == file_time:
        with open(pkl_file, 'rb') as handle:
            dt_excel= pkl.load(handle)
        return dt_excel, "[{0}] Leído correctamente".format(file_name)


def create_datetime_and_span(ini_date: dt.datetime, end_date: dt.datetime):

    """ Create time_range """
    try:
        time_range_aux = pi_svr.time_range(str(ini_date), str(end_date))
    except Exception as e:
        return None, None, "No se puede crear el time_range con los siguientes parámetros [{0}, {1}]" \
            .format(ini_date, end_date)

    """ Create Span value """
    span_aux = end_date - ini_date
    try:
        span_aux = max(1, span_aux.days)
        span_aux = pi_svr.span(str(span_aux) + "d")
    except Exception as e:
        return None, None, "No se pudo calcular el span [{0}, {1}] \n ".format(ini_date, end_date) + str(e)

    return time_range_aux, span_aux, "Cálculo correcto de time_range y span [{0}, {1}, {2}]".format(ini_date, end_date, span_aux)
