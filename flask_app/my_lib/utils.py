import argparse, os
import datetime as dt
import hashlib
import json
import traceback
from random import randint
import pandas as pd
import pickle as pkl

script_path = os.path.dirname(os.path.abspath(__file__))


def valid_date(s):
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "El parámetro: '{0}' no es una fecha válida, (formato YYYY-MM-DD).".format(s)
        raise argparse.ArgumentTypeError(msg)


def valid_date_h_m_s(s):
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        msg = "El parámetro: '{0}' no es una fecha válida, (formato YYYY-MM-DD).".format(s)
        raise argparse.ArgumentTypeError(msg)


def get_dates_for_last_month():
    d_n = dt.datetime.now()
    date_ini = dt.datetime(year=d_n.year, month=d_n.month - 1, day=1)
    date_end = dt.datetime(year=d_n.year, month=d_n.month, day=d_n.day) - dt.timedelta(days=d_n.day)
    return date_ini, date_end


def get_dates_by_default():
    today = dt.datetime.now()
    today = dt.datetime(year=today.year, month=today.month, day=today.day)
    end_date = today
    if today.day > 1:
        ini_date = today - dt.timedelta(days=today.day - 1)
    else:
        yesterday = today - dt.timedelta(days=1)
        ini_date = yesterday - dt.timedelta(days=yesterday.day - 1)
    return ini_date, end_date


def get_last_day():
    today = dt.datetime.now()
    today = dt.datetime(year=today.year, month=today.month, day=today.day)
    yesterday = today - dt.timedelta(days=1)
    return yesterday, today


def check_date(s):
    if isinstance(s, dt.datetime):
        return True, s
    success, date = check_date_yyyy_mm_dd(s)
    if success:
        return success, date
    success, date = check_date_yyyy_mm_dd_hh_mm_ss(s)
    if success:
        return success, date


def check_date_yyyy_mm_dd(s):
    try:
        return True, dt.datetime.strptime(s, "%Y-%m-%d")
    except Exception as e:
        return False, str(e)


def check_date_yyyy_mm_dd_hh_mm_ss(s):
    try:
        return True, dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception as e:
        return False, str(e)


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
            dt_excel = pkl.load(handle)
        return dt_excel, "[{0}] Leído correctamente".format(file_name)


def group_files(repo, files):
    # let´s group files with similar name
    to_work = [os.path.splitext(f)[0] for f in files]
    groups = sorted(list(set([n.split("@")[0] for n in to_work])))
    done_list = list()
    result = dict()
    for ix, group in enumerate(to_work):
        if group in groups and to_work.index(group) not in done_list:
            result[group] = [files[to_work.index(group)]]
            done_list.append(to_work.index(group))
        else:
            continue
        rest_list = [i for i in range(len(to_work)) if i not in done_list]
        for iy in rest_list:
            if group in to_work[iy] or to_work[iy] in group:
                done_list.append(iy)
                result[group].append(files[iy])

    rest_list = [i for i in range(len(to_work)) if i not in done_list]
    if len(rest_list) > 0:
        for r in rest_list:
            result[files[r]] = [files[r]]
    final = dict()
    for k in sorted(result.keys()):
        final[k] = [dict(name=file,
                         datetime=str(dt.datetime.fromtimestamp(os.path.getmtime(os.path.join(repo, file)))))
                    for file in result[k]]

    return final


def get_id(params: list):
    id = str()
    for p in params:
        id += str(p).lower().strip()
    return hashlib.md5(id.encode()).hexdigest()


def retrieve_from_file(temp_file, id):
    if os.path.exists(temp_file):
        with open(temp_file) as json_file:
            resp = json.load(json_file)
            size = os.path.getsize(temp_file)
            if size > 10 * 1024 * 1024:
                os.remove(temp_file)
            return resp.pop(id, None)
    else:
        return None


def save_in_file(temp_file, id, data_dict):
    if not os.path.exists(temp_file):
        to_save = {id: data_dict}
    else:
        with open(temp_file) as json_file:
            to_save = json.load(json_file)
            to_save.update({id: data_dict})

    with open(temp_file, 'w') as outfile:
        json.dump(to_save, outfile, indent=4, sort_keys=True)


def is_active(path_file, id: str, time_delta: dt.timedelta):
    try:
        value_dict = retrieve_from_file(path_file, id)
        if value_dict is None:
            return False
        else:
            value_dict["fecha"] = dt.datetime.strptime(value_dict["fecha"], "%Y-%m-%d %H:%M:%S")
            if value_dict["fecha"] + time_delta > dt.datetime.now():
                return value_dict["activo"]
            else:
                return False

    except Exception as e:
        tb = traceback.extract_stack()
        print(f"{str(e)} \n {tb}")
        return True


# determina si debe usar el reporte temporal o el reporte definitivo:
def isTemporal(ini_date: dt.datetime, end_date: dt.datetime):
    delta = end_date - ini_date
    return delta.days < 27 and delta.days != 7


def get_block(from_label: str, to_label: str, html_str: str):
    str_result = str()
    from_index = html_str.find(from_label)
    to_index = html_str.find(to_label)
    if from_index > 0 and to_index > 0:
        str_result = html_str[from_index + len(from_label): to_index]
    return str_result


def replace_block(from_label: str, to_label: str, html_str: str, to_replace: str):
    str_result = html_str
    from_index = html_str.find(from_label)
    to_index = html_str.find(to_label)
    if from_index > 0 and to_index > 0:
        str_result = html_str[:from_index] + to_replace + html_str[to_index:]
        str_result = str_result.replace(to_label, "")
    return str_result


def save_html(html_str, path_html_to_save):
    # Guardar el archivo html en la carpeta reportes:
    try:
        Html_file = open(path_html_to_save, "w", encoding='utf-8')
        Html_file.write(html_str)
        Html_file.close()
    except Exception as e:
        print(e)


def set_max_age_to_response(response, minutes):
    response.expires = dt.datetime.utcnow() + dt.timedelta(minutes=minutes)
    response.cache_control.max_age = minutes * 60
    return response


def find_entity_in_node(node, id_entity):
    idx = None
    entidades = node["entidades"] if isinstance(node, dict) else node.entidades
    for ix, _entidad in enumerate(entidades):
        id_entidad = _entidad["id_entidad"] if isinstance(_entidad, dict) else _entidad.id_entidad
        if id_entidad == id_entity:
            idx = ix
            break
    return idx is not None, idx


def check_if_all_are_there(reference, to_check):
    no_ok = list()
    for ref in reference:
        if ref not in list(to_check):
            no_ok.append(ref)
    return len(no_ok) == 0, no_ok


def write_temporal_file(streamed_file, temp_path):
    filename = streamed_file.filename
    stream_excel_file = streamed_file.stream.read()
    # path del archivo temporal a guardar para poderlo leer inmediatamente
    temp_file = os.path.join(temp_path, f"{str(randint(0, 100))}_{filename}")
    with open(temp_file, 'wb') as f:
        f.write(stream_excel_file)
    return os.path.exists(temp_file), temp_file


def get_df_from_excel_streamed_file(streamed_file, temp_path):
    if streamed_file.mimetype in 'application/xls, application/vnd.ms-excel,  application/xlsx' \
                                      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        try:
            success, temp_file = write_temporal_file(streamed_file, temp_path)
            if not success:
                return False, "No fue posible subir este archivo"
            # obtener el dataframe:
            df = pd.read_excel(temp_file, engine='openpyxl')
            # una vez leído, eliminar archivo temporal
            os.remove(temp_file)
            return True, df, "Archivo leído correctamente"
        except Exception as e:
            msg = f"Error al leer información del archivo: {str(e)}"
            return False, pd.DataFrame(), msg


def replace_edit_tags_in_node(nodo, idx, id_utr, tags_req):
    from flask_app.dto.mongo_engine_handler.sRNode import SRTag
    for ix, _utr in enumerate(nodo.entidades[idx].utrs):
        if _utr.id_utr == id_utr or _utr.utr_code == id_utr:
            tag_names = [t.pop("tag_name_original", None) for t in tags_req]
            [t.pop("edited", None) for t in tags_req]
            SRTags = [SRTag(**d) for d in tags_req]
            success, (n_remove, msg) = nodo.entidades[idx].utrs[ix].remove_tags(tag_names)
            if not success:
                return False, None, msg
            success, msg = nodo.entidades[idx].utrs[ix].add_or_replace_tags(SRTags)
            if success:
                nodo.save()
                tags = [t.to_dict() for t in nodo.entidades[idx].utrs[ix].tags]
                return True, tags, f"{nodo.entidades[idx].utrs[ix]} TAGS: -editadas: {n_remove} " \
                             f"-añadidas: {len(tags_req) - n_remove}"
            return False, None, f"{nodo.entidades[idx].utrs[ix]} {msg}"

def check_range_yyyy_mm_dd_hh_mm_ss(ini_date, end_date):
    success1, ini_date = check_date_yyyy_mm_dd_hh_mm_ss(ini_date)
    success2, end_date = check_date_yyyy_mm_dd_hh_mm_ss(end_date)
    if not success1 or not success2:
        msg = "No se puede convertir: " + (ini_date if not success1 else end_date)
        return False, ini_date, end_date, msg
    if end_date < ini_date:
        return False, ini_date, end_date, "La fecha de inicio es mayor que la fecha de fin"
    return True, ini_date, end_date, "Rango correcto"