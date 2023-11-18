cl_activado = "activado"

cl_utr = "utr"
cl_utr_nombre = "utr_nombre"
cl_utr_tipo = "utr_tipo"
cl_entidad_nombre = "entidad_nombre"
cl_entidad_tipo = "entidad_tipo"
cl_protocolo = "protocolo"
cl_longitud = "longitud"
cl_latitud = "latitud"

cl_tag_name = "tag_name"
cl_filter_expression = "filter_expression"

cl_nivel_voltaje = "voltaje"
cl_bahia_code = "bahia_code"
cl_filter_tag = "filter_tag"
cl_instalacion_ems_code = "instalacion_ems_code"
cl_instalacion_nombre = "instalacion_nombre"
cl_instalacion_tipo = "instalacion_tipo"
cl_bahia_tipo = "bahia_tipo"
cl_bahia_nombre = "bahia_nombre"
cl_activado_bahia = "activado_bahia"
cl_activado_tag = "activado_tag"

lb_bahia = "Bahía"

default_filter_expression = 'TE.*#ME.*#*.ME#*.TE# DA.*#*.DA#*.INV#INV.*#TE#ME#DA#INV'

tag_migration_columns = [cl_tag_name, cl_instalacion_ems_code, cl_nivel_voltaje, cl_bahia_code, cl_filter_tag]

v1_main_sheet_columns = [cl_utr, cl_utr_nombre, cl_utr_tipo,
                         cl_entidad_tipo, cl_protocolo, cl_longitud, cl_latitud, cl_activado]

v1_tags_sheet_columns = [cl_utr, cl_tag_name, cl_filter_expression, cl_activado]

v2_tags_sheet_columns = [cl_instalacion_ems_code, cl_bahia_code, cl_nivel_voltaje, cl_tag_name, cl_filter_expression,
                         cl_activado]

v2_main_sheet_columns = [cl_entidad_tipo, cl_entidad_nombre, cl_instalacion_ems_code, cl_instalacion_tipo,
                         cl_instalacion_nombre,
                         cl_protocolo, cl_latitud, cl_longitud, cl_activado]

v2_bahias_sheet_columns = [cl_instalacion_ems_code, cl_bahia_code, cl_nivel_voltaje, cl_bahia_nombre, cl_activado]

v2_entidad_properties = [cl_entidad_tipo, cl_entidad_nombre, cl_activado]

v2_instalacion_properties = [cl_instalacion_ems_code, cl_instalacion_tipo, cl_instalacion_nombre,
                             cl_protocolo, cl_latitud, cl_longitud, cl_activado]

v2_tag_properties = [cl_tag_name, cl_filter_expression, cl_activado]
v2_tag_dict_values = [cl_tag_name, cl_filter_expression, cl_activado_tag]