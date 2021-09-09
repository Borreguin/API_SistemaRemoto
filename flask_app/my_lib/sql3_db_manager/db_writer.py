import os
import sqlite3
import traceback
import pandas as pd
import hashlib

cl_empresa = "empresa"
cl_activado = "activado"
cl_entidades = "entidades"
cl_tag_name = "tag_name"
cl_f_expression = "filter_expression"
cl_indisp_acc = "indisp_acumulada_minutos"
cl_disp_avg = "disp_promedio_minutos"
cl_perc_disp = "porcentage_disponibilidad"
cl_disp_ponderada = "disponibilidad_promedio_ponderada_minutos"
cl_utr = "utr"
cl_tags_problema = "tags_con_novedades"
cl_n_tags = "no_seniales"
cl_name = "entidad_nombre"
cl_weight = "peso"
cl_period_ini = "fecha_inicio"
cl_period_end = "fecha_final"
cl_n_minutos = "periodo_minutos"
cl_u_negocio = "unidad_negocio"
cl_json = "json_info"
cl_configuration_archive = "archivo_configuración"

script_path = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.dirname(script_path)
db_path = os.path.join(db_path, "_db")


class DBWriterDisponibilidad:
    def __init__(self, filename=None):
        global db
        if filename is None:
            filename = os.path.join(db_path, "disponibilidad.db")
        # Our custom argument
        db = sqlite3.connect(filename)  # might need to use self.filename
        sql_str = f"CREATE TABLE IF NOT EXISTS detalle(id text NOT NULL UNIQUE, " \
                  f"{cl_empresa} text, {cl_entidades} text, {cl_name} text, {cl_n_tags} integer, " \
                  f"{cl_period_ini} text, {cl_period_end} text, {cl_n_minutos} integer, " \
                  f"{cl_disp_avg} numeric, " \
                  f"{cl_weight} numeric, {cl_perc_disp} numeric, {cl_json} text)"
        db.execute(sql_str)
        db.commit()

        sql_str = f"CREATE TABLE IF NOT EXISTS resumen(id text NOT NULL UNIQUE, " \
                  f"{cl_configuration_archive} text,  {cl_n_tags} integer, {cl_period_ini} text, " \
                  f"{cl_period_end} text, " \
                  f"{cl_n_minutos} integer, {cl_disp_ponderada} numeric, {cl_json} text)"
        db.execute(sql_str)
        db.commit()

    def insert_or_replace_details(self, df_details: pd.DataFrame):
        columns = [cl_period_ini, cl_period_end, cl_n_minutos, cl_empresa, cl_entidades, cl_name,
                   cl_n_tags, cl_disp_avg, cl_weight, cl_perc_disp, cl_json]
        columns.sort()
        c_val = 0
        for c in columns:
            if c in df_details.columns:
                c_val += 1
        if c_val != len(columns):
            s_columns = list(df_details.columns)
            s_columns.sort()
            return False, "Columnas no válidas en DataFrame: " \
                          "\nActual   : {0}" \
                          "\nEsperado : {1}".format(s_columns, columns)

        lst_error = list()
        for ix in df_details.index:
            code = df_details[cl_period_ini].loc[ix] + df_details[cl_period_end].loc[ix] + \
                   df_details[cl_empresa].loc[ix] + df_details[cl_entidades].loc[ix]
            hash_value = hashlib.md5(code.encode()).hexdigest()
            try:
                sql_str = f'INSERT OR REPLACE INTO detalle(id, {cl_empresa}, {cl_entidades}, {cl_name}, ' \
                          f'{cl_n_tags}, {cl_period_ini}, {cl_period_end}, ' \
                          f'{cl_n_minutos}, {cl_disp_avg}, {cl_weight}, ' \
                          f'{cl_perc_disp}, {cl_json}) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)'
                db.execute(sql_str,
                           (
                               hash_value,
                               df_details[cl_empresa].loc[ix],
                               df_details[cl_entidades].loc[ix],
                               df_details[cl_name].loc[ix],
                               df_details[cl_n_tags].loc[ix],
                               df_details[cl_period_ini].loc[ix],
                               df_details[cl_period_end].loc[ix],
                               df_details[cl_n_minutos].loc[ix],
                               df_details[cl_disp_avg].loc[ix],
                               df_details[cl_weight].loc[ix],
                               df_details[cl_perc_disp].loc[ix],
                               df_details[cl_json].loc[ix]
                           )
                           )
                db.commit()
            except Exception as e:
                msg = "({0}): {1} \t{2} \n {3}".format(hash_value, code, str(e), traceback.format_exc())
                lst_error.append(msg)
        if len(lst_error) > 0:
            return False, "No se pudieron insertar las filas:\n[{0}]".format("\n".join(lst_error))
        else:
            return True, f"El detalle fue correctamente insertado en base de datos: [{db_path}][detalle]"

    def insert_or_replace_summary(self, df_summary: pd.DataFrame):
        columns = [cl_period_ini, cl_period_end, cl_n_minutos, cl_configuration_archive,
                   cl_n_tags, cl_perc_disp, cl_json]
        columns.sort()
        c_val = 0
        for c in columns:
            if c in df_summary.columns:
                c_val += 1
        if c_val != len(columns):
            s_columns = list(df_summary.columns)
            s_columns.sort()
            return False, "Columnas no válidas en DataFrame: " \
                          "\nActual   : {0}" \
                          "\nEsperado : {1}".format(s_columns, columns)

        lst_error = list()
        for ix in df_summary.index:
            code = df_summary[cl_period_ini].loc[ix] + df_summary[cl_period_end].loc[ix] + \
                   df_summary[cl_configuration_archive].loc[ix]
            hash_value = hashlib.md5(code.encode()).hexdigest()
            try:
                db.execute(f'INSERT OR REPLACE INTO resumen(id, {cl_configuration_archive}, {cl_n_tags}, '
                           f'{cl_period_ini}, {cl_period_end}, {cl_n_minutos}, {cl_disp_ponderada}, {cl_json}) '
                           'VALUES(?,?,?,?,?,?,?,?)',
                           (
                               hash_value,
                               df_summary[cl_configuration_archive].loc[ix],
                               df_summary[cl_n_tags].loc[ix],
                               df_summary[cl_period_ini].loc[ix],
                               df_summary[cl_period_end].loc[ix],
                               df_summary[cl_n_minutos].loc[ix],
                               df_summary[cl_disp_ponderada].loc[ix],
                               df_summary[cl_json].loc[ix]
                           ))
                db.commit()
            except Exception as e:
                msg = "({0}): {1} \t{2} \n {3}".format(hash_value, code, str(e), traceback.format_exc())
                lst_error.append(msg)

        if len(lst_error) > 0:
            return False, "No se pudieron insertar las filas ({0})".format("\n".join(lst_error))
        else:
            return True, f"El resumen fue correctamente insertado en base de datos: [{db_path}][resumen]"
