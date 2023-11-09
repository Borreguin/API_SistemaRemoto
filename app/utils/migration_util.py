from __future__ import annotations

import os.path
from typing import List, Tuple

import pandas as pd
import re

from app.utils.excel_constants import *

regex_numbers = re.compile("([0-9]+\\.[0-9]+|[0-9]+)")
regex_ems_code_1 = re.compile("^[^#]*")
regex_ems_code_2 = re.compile("^\\w{1,8}")
regex_bahia_code = re.compile("\\w{1,12}")
max_voltage_detection = 500
min_voltage_detection = 10
max_bahia_len = 7
voltages_final = []
first = 0
last = -1

default_voltage_levels = [500, 230, 138, 69, 34.5, 34, 24.16, 13.8, 13, 6.6, 4.6, 4.2, 4.16, 4.1, 0.62, 0.48]
end_token_bahia = ['_SC\\.', '_TIE', '_TE_', '_ALA\\.', '_IT\\.', '_I\\.', '_V\\.', '_P\\.', '_Q\\.', '_HERTZ', '_BUC',
                   '_CON', '_IT\\.L-C', '\\.L-C', '_FLG', '_DIS', '_INDS', '_FAL', '_CAUD', '_LIM', '\\.LTC', '_PAR',
                   '_SMD', '_SME', '_NIVEL', '_PORC', '_MODO', '_FREC', '_SPC', '#', '']
bahia_between_tokens = ['#([\\w*|\\s*|-]{3,})#']
bahia_regex_list = [re.compile(regex) for regex in bahia_between_tokens] + [re.compile("([\\w|-]{1,9})" + end) for end
                                                                            in end_token_bahia]


def get_first_half(tag_name: str):
    return tag_name[:len(tag_name) // 2]


def get_all_possible_voltage_levels(to_check: List[str]):
    global default_voltage_levels
    to_check = " ".join([get_first_half(s) for s in to_check])
    numbers = set(regex_numbers.findall(to_check))
    float_numbers = [float(n) for n in numbers if '.' in n and float(n) not in default_voltage_levels]
    int_numbers = [int(n) for n in numbers if '.' not in n and int(n) not in default_voltage_levels
                   and max_voltage_detection >= int(n) >= min_voltage_detection]
    default_voltage_levels += int_numbers + float_numbers
    return default_voltage_levels


def remove_voltage(tag: str, voltage_levels: List[int | float]):
    mid_tag = get_first_half(tag)
    voltage = [v for v in voltage_levels if str(v) in mid_tag]
    if len(voltage) == 0:
        return tag, None
    filter_tag = tag.replace(str(voltage[first]), "#", 1)
    return filter_tag, voltage[first]


def filter_voltage_level(df_group: pd.DataFrame, df_filter: pd.DataFrame, voltage_levels) -> pd.DataFrame:
    for ix in df_group.index:
        filter_tag, voltage = remove_voltage(df_group[cl_tag_name].loc[ix], voltage_levels)
        df_filter[cl_filter_tag].loc[ix] = filter_tag
        df_filter[cl_nivel_voltaje].loc[ix] = voltage
    return df_filter


def detect_code(tag_name: str, regex: re.Pattern, max_len: int):
    code = regex.findall(tag_name)
    if len(code) > 0:
        resp = code[first][:max_len]
        return True, resp
    return False, None


def detect_code_by_regex_list(tag: str, regex_list: List[re.Pattern], max_len=8) -> Tuple[bool, str, str]:
    for regex in regex_list:
        spotted, code = detect_code(tag, regex, max_len)
        if spotted:
            return True, code, tag.replace(code, "#", 1)
    return False, '', ''


def detect_bahia_by_regex_list(tag: str) -> Tuple[bool, str, str]:
    tag = tag.replace(" ", "")
    for regex in bahia_regex_list:
        spotted, bahia_code = detect_code(tag, regex, 12)
        if spotted:
            bahia_code = clean_bahia_code(bahia_code)
            return True, bahia_code, tag.replace(bahia_code, "#", 1)
    return False, '', ''


def filter_instalacion_ems_code(df_group: pd.DataFrame, df_filter: pd.DataFrame) -> pd.DataFrame:
    for ix in df_group.index:
        spotted, ems_code, filter_tag = spot_instalacion_ems_code(df_filter[cl_instalacion_ems_code].loc[ix],
                                                                  df_filter[cl_filter_tag].loc[ix])
        if spotted:
            df_filter[cl_filter_tag].loc[ix] = filter_tag
            df_filter[cl_instalacion_ems_code].loc[ix] = ems_code

    return df_filter


def spot_instalacion_ems_code(instalacion_ems_code: str, tag: str):
    if instalacion_ems_code in tag and len(instalacion_ems_code) > 0:
        return True, instalacion_ems_code, tag.replace(instalacion_ems_code, "#", 1)
    spotted, ems_code, filter_tag = detect_code_by_regex_list(tag, [regex_ems_code_1, regex_ems_code_2])
    if spotted and len(instalacion_ems_code) > 0:
        return True, instalacion_ems_code, filter_tag
    if spotted:
        return True, ems_code, filter_tag
    return False, None, None


def clean_bahia_code(bahia_code: str):
    if bahia_code is None or len(bahia_code) < 2:
        return bahia_code
    if bahia_code[first] == "_" or bahia_code[first] == "-":
        bahia_code = bahia_code[1:]
    if bahia_code[last] == "_" or bahia_code[last] == "-":
        bahia_code = bahia_code[:-1]
    return bahia_code.strip()


def spot_bahia_code(filter_tag: str) -> Tuple[bool, str, str]:
    spotted, bahia_code, filter_tag = detect_bahia_by_regex_list(filter_tag)
    return spotted, bahia_code, filter_tag


def filter_bahia_code(df_group: pd.DataFrame, df_filter: pd.DataFrame) -> pd.DataFrame:
    for ix in df_group.index:
        spotted, bahia_code, filter_tag = spot_bahia_code(df_filter[cl_filter_tag].loc[ix])
        if spotted:
            df_filter[cl_bahia_code].loc[ix] = bahia_code
            df_filter[cl_filter_tag].loc[ix] = filter_tag
            continue
    return df_filter


def migration_process_df_tag(df_tag: pd.DataFrame) -> pd.DataFrame:
    voltage_levels = get_all_possible_voltage_levels(list(df_tag[cl_tag_name]))
    df_filter = pd.DataFrame(columns=tag_migration_columns)
    df_tag[cl_instalacion_ems_code] = df_tag[cl_utr]

    for ems_code, df_group in df_tag.groupby(by=[cl_instalacion_ems_code]):
        df_filter_group = pd.DataFrame(columns=tag_migration_columns)
        df_filter_group[cl_tag_name] = df_group[cl_tag_name]
        df_filter_group[cl_instalacion_ems_code] = df_group[cl_instalacion_ems_code]
        # Detect voltage level
        df_filter_group = filter_voltage_level(df_group, df_filter_group, voltage_levels)
        # Detect ems code
        df_filter_group = filter_instalacion_ems_code(df_group, df_filter_group)
        # Detect bahia code
        df_filter_group = filter_bahia_code(df_group, df_filter_group)
        # Add detected values to the final dataframe
        df_filter = pd.concat([df_filter, df_filter_group], ignore_index=True)
    df_filter[cl_activado] = 'x'
    df_filter[cl_filter_expression] = default_filter_expression
    return df_filter[v2_tags_sheet_columns]


def migration_process_df_main(df_main: pd.DataFrame) -> pd.DataFrame:
    df_result = df_main.copy()
    df_result.rename(columns={cl_utr: cl_instalacion_ems_code, cl_utr_tipo: cl_instalacion_tipo,
                              cl_utr_nombre: cl_instalacion_nombre}, inplace=True)
    return df_result[v2_main_sheet_columns]


def migration_process_df_bahia(df_tags: pd.DataFrame) -> pd.DataFrame:
    df_result = pd.DataFrame(columns=v2_bahias_sheet_columns)
    group_by = [cl_instalacion_ems_code, cl_nivel_voltaje, cl_bahia_code]
    for (ems_code, voltage, bahia_code), df_group in df_tags.groupby(by=group_by):
        row = {cl_instalacion_ems_code: ems_code, cl_bahia_code: bahia_code, cl_bahia_tipo: lb_bahia,
               cl_bahia_nombre: bahia_code, cl_nivel_voltaje: voltage, cl_activado: 'x'}
        df_aux = pd.DataFrame(data=row, index=[0])
        df_result = pd.concat([df_result, df_aux], ignore_index=True)
    return df_result


def v2_write_excel_file_settings(df_main: pd.DataFrame, df_bahia: pd.DataFrame, df_tags: pd.DataFrame, path_file):
    with pd.ExcelWriter(path_file) as writer:
        df_main.to_excel(writer, sheet_name="main")
        df_bahia.to_excel(writer, sheet_name="bahias")
        df_tags.to_excel(writer, sheet_name="tags")
    return os.path.exists(path_file)