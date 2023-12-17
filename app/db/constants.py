V1_SR_NODE_LABEL = "SRNode"
V2_SR_NODE_LABEL = "SRNode_v2"
V2_SR_ENTITY_LABEL = "SREntity_v2"
V2_SR_INSTALLATION_LABEL = "SRInstallationV2"
V2_SR_CONSIGNMENT_LABEL = "v2SRConsignments"

SR_NODE_COLLECTION = "CONFG|Nodos"
SR_INSTALLATION_COLLECTION = "CONFG|Instalaciones"

attr_nombre = "nombre"
attr_tipo = "tipo"
att_activado = "activado"
attributes_node = [attr_nombre, attr_tipo, att_activado]

attr_entidad_nombre = "entidad_nombre"
attr_entidad_tipo = "entidad_tipo"
attr_entidad_activado = "activado"
attributes_entity = [attr_entidad_nombre, attr_entidad_tipo, attr_entidad_activado]

attr_id_entidad = "id_entidad"
attr_entidades = "entidades"

attr_instalacion_ems_code = 'instalacion_ems_code'
attr_instalacion_nombre = 'instalacion_nombre'
attr_instalacion_tipo = 'instalacion_tipo'
attr_consignaciones = 'consignaciones'
attr_activado = 'activado'
attr_protocolo = 'protocolo'
attr_longitud = 'longitud'
attr_latitud = 'latitud'
attr_bahias = 'bahias'
attr_actualizado = 'actualizado'

attributes_editable_installation = [attr_instalacion_ems_code, attr_instalacion_nombre, attr_instalacion_tipo,
                                    attr_activado, attr_protocolo, attr_longitud, attr_latitud]

lb_n_tags = "n_tags"
lb_n_bahias = "n_bahias"
lb_n_instalaciones = "n_instalaciones"
lb_n_entidades = "n_entidades"
lb_consignments = "consignments"


attr_no_consignacion = "no_consignacion"
attr_fecha_inicio = "fecha_inicio"
attr_fecha_final = "fecha_final"
attr_detalle = "detalle"
attr_responsable = "responsable"
attributes_consignments = [attr_no_consignacion, attr_fecha_inicio, attr_fecha_final, attr_detalle, attr_responsable]