""" coding: utf-8
Created by rsanchez on 15/08/2018
Este proyecto ha sido desarrollado en la Gerencia de Operaciones de CENACE
Mateo633
"""
import os
import pickle
import datetime as dt
import pymongo as pm
script_path = os.path.dirname(os.path.abspath(__file__))
max_num_files = 1000
path_temporal_db = 'E:\\GOP_WebServer_mongo_db\\db'
mongo_exe_path = 'C:\\Program Files\\MongoDB\\Server\\3.6\\bin\\mongod.exe'
bat_path_file = 'E:\\GOP_WebServer_mongo_db\\start_mongo_db.bat'


def root_path():
    return script_path.replace('my_lib\\temporal_files_manager', "")


def temporal_path():
    return root_path() + 'static\\temp\\'


def retrieve_file(file_name, eval_time=None):
    """
    La funcion regresa el archivo temporal especificado en
    file_name si eval_time se encuentra dentro del rango de tiempo válido.
    El rango de tiempo válido es especificado al momento de guardar el arhivo temporal
    (función: save_variables)
    :param file_name:  Nombre del archivo temporal
    :param eval_time:  Tiempo en que se verifica si el archivo temporal es aùn vàlido
    :return: datos de archivo temporal
    """
    file_path = temporal_path() + file_name
    file_path = valid_path(file_path)

    if eval_time is None and os.path.exists(file_path):
        # eval_time = dt.datetime.fromtimestamp(os.path.getmtime(file_path))
        eval_time = dt.datetime.now()

    if isinstance(eval_time, dt.timedelta):
        print("version antigua de temporal para " + file_name)
        return SyntaxError

    if os.path.exists(file_path) and isinstance(eval_time, dt.datetime):

        with open(file_path, 'rb') as f:
            resp = pickle.load(f)

        list_objects = resp["list_objects"]
        valid_range = resp["valid_range"]

        if valid_range[0] is None and valid_range[1] is None:
            empty_temp_files(max_num_files)
            return list_objects

        if valid_range[0] is not None and valid_range[1] is None:
            if eval_time >= valid_range[0]:
                empty_temp_files(max_num_files)
                return list_objects

        if valid_range[0] <= eval_time <= valid_range[1]:
            empty_temp_files(max_num_files)
            return list_objects
        else:
            return None

    else:
        return None


def empty_temp_files(limit_number_of_files):
    tmp_path = temporal_path()
    file_list = os.listdir(tmp_path)
    if limit_number_of_files < len(file_list):
        for f in file_list:
            os.remove(os.path.join(temporal_path(), f))
    return len(file_list)


def save_variables(file_name, list_objects, valid_range=None, dt_delta=None):

    file_path = temporal_path() + file_name
    file_path = valid_path(file_path)
    dt_n = dt.datetime.now()
    if valid_range is None and dt_delta is None:
        valid_range = [dt_n, dt_n + dt.timedelta(minutes=2)]
    elif isinstance(dt_delta, dt.timedelta):
        valid_range = [dt_n, dt_n + dt_delta]

    try:
        # Saving the objects:
        with open(file_path, 'wb') as f:
            to_dump = dict(list_objects=list_objects, valid_range=valid_range)
            pickle.dump(to_dump, f)
        return True
    except Exception as e:
        print(e)
        return False


def valid_path(file_path):
    file_path = file_path[:3] + file_path[3:].replace(":", "_")
    file_path = file_path[:3] + file_path[3:].replace("/", "_")
    return file_path


def start_temporal_db():
    import subprocess
    import os
    to_execute = mongo_exe_path + " --dbpath=" + path_temporal_db
    try:
        p = subprocess.Popen(to_execute)
        print("Base de datos temporal corre de manera correcta")
    except Exception as e:
        print(e)


def save_dict_in_cal_db(collection_name, cal_id, data_dict):
    try:
        client = pm.MongoClient()
        data_dict['cal_id'] = cal_id
        db = client["cal_db"]
        collection = db[collection_name]
        if collection.count() == 0:
            collection.create_index([('cal_id', pm.ASCENDING)], unique=True)
        collection.insert_one(data_dict)
        if u'_id' in data_dict.keys() or '_id' in data_dict.keys():
            data_dict[u'_id'] = str(data_dict[u'_id'])
        return True, "Cálculo ingresado de manera correcta"

    except Exception as e:
        print(e)
        return False, str(e)


def retrieve_dict_in_cal_db(collection_name, cal_id):
    try:
        client = pm.MongoClient()
        db = client["cal_db"]
        collection = db[collection_name]
        result = collection.find_one({'cal_id': cal_id}, {'_id': False})
        if u'_id' in result.keys() or '_id' in result.keys():
            result[u'_id'] = cal_id
        return True, result
    except Exception as e:
        return False, str(e)


def test():
    # Manejo de archivos temporales:
    # cálculo disponible por 1 minuto:
    dt_delta = dt.timedelta(minutes=1)
    timestamp = dt.datetime.now()
    rt_value = retrieve_file("temporal_manager_test.pkl", timestamp)
    if rt_value is not None:
        print("El archivo temporal existe y es todavía válido")
        print(rt_value)
    else:
        # recalculate after 1 minute:
        print("El archivo debe ser nuevamente calculado")
        aux_1 = str(dt.datetime.now())
        aux_2 = "value2"
        aux_3 = "value3"
        # archivo válido en 3 días:
        valid_range = [timestamp, timestamp + dt.timedelta(days=3)]
        print("Archivo válido en el periodo: " + str(valid_range))
        # forma de uso No. 1
        save_variables("temporal_manager_test.pkl", list_objects=[aux_1, aux_2, aux_3],
                       valid_range=valid_range)
        # forma de uso No. 2
        save_variables("temporal_manager_test.pkl", list_objects=[aux_1, aux_2, aux_3],
                       valid_range=None, dt_delta=dt.timedelta(days=3))

    # Manejo de datos guradados en base de datos (cal_db)
    collection_test = "collection_test"
    cal_id = "test"
    data = dict(value_1="Este",value_2="es un dato ha ser conservado en cal_db")
    save_dict_in_cal_db(collection_test, cal_id, data)

    result = retrieve_dict_in_cal_db(collection_test, cal_id)
    print(result)


if __name__ == "__main__":
    perform_test = True
    if perform_test:
        print(test())
        # start_temporal_db()


