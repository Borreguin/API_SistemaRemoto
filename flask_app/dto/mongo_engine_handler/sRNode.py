# Created by Roberto Sanchez at 21/04/2020
# -*- coding: utf-8 -*-

"""
    Clases que relacionan los documentos JSON de la base de datos de MongoDB con los objetos creados
    Object mapper for SRNodes

"""
import math

from flask_app.dto.mongo_engine_handler.Consignment import *
import datetime as dt
import pandas as pd


class SRTag(EmbeddedDocument):
    tag_name = StringField(required=True)
    filter_expression = StringField(required=True)
    activado = BooleanField(default=True)

    def __str__(self):
        return f"{self.tag_name}: {self.activado}"

    def to_dict(self):
        return dict(tag_name=self.tag_name, filter_expression=self.filter_expression, activado=self.activado)


class SRUTR(EmbeddedDocument):
    id_utr = StringField(required=True, sparse=True, default=None)
    utr_nombre = StringField(required=True)
    utr_tipo = StringField(required=True)
    tags = ListField(EmbeddedDocumentField(SRTag))
    consignaciones = ReferenceField(Consignments, dbref=True)
    utr_code = StringField(required=True, default=None)
    activado = BooleanField(default=True)
    protocol = StringField(default="No definido", required=False)  # Nuevo atributo
    longitude = FloatField(required=False, default=0)  # Nuevo atributo
    latitude = FloatField(required=False, default=0)  # Nuevo atributo

    def __init__(self, *args, **values):
        super().__init__(*args, **values)
        # check if there are consignaciones related with this element:
        if self.utr_code is None:
            id = str(self.id_utr).lower().strip()
            self.utr_code = hashlib.md5(id.encode()).hexdigest()
            # self.create_consignments_container()

    def create_consignments_container(self):
        try:
            if self.consignaciones is None:
                # if there are not consignments then create a new document
                # relate an existing consignacion
                consignments = Consignments.objects(id_elemento=self.utr_code).first()
                if consignments is None:
                    consignments = Consignments(id_elemento=self.utr_code,
                                                elemento=self.to_summary())
                    consignments.save()
                self.consignaciones = consignments
                return True, consignments
            else:
                return True, self.consignaciones
        except Exception as e:
            return False, None

    def add_or_replace_tags(self, tag_list: list):
        # check si todas las tags son de tipo SRTag
        check_tags = [isinstance(t, SRTag) for t in tag_list]
        if not all(check_tags):
            lg = [str(tag_list[i]) for i, v in enumerate(check_tags) if not v]
            return False, [f"La siguiente lista de tags no es compatible:"] + lg

        # unificando las lista y crear una sola
        unique_tags = dict()
        unified_list = self.tags + tag_list
        for t in unified_list:
            unique_tags.update({t.tag_name: t})
        self.tags = [unique_tags[k] for k in unique_tags.keys()]
        return True, "Insertada las tags de manera correcta"

    def remove_tags(self, tag_list: list):
        # check si todas las tags son de tipo str
        check_tags = [isinstance(t, str) for t in tag_list]
        if not all(check_tags):
            lg = [str(tag_list[i]) for i, v in enumerate(check_tags) if not v]
            return False, (0, [f"La siguiente lista de tags no es compatible:"] + lg)
        n_remove = 0
        for tag in tag_list:
            new_list = [t for t in self.tags if t.tag_name != tag]
            if len(new_list) != len(self.tags):
                n_remove += 1
            self.tags = new_list
        return True, (n_remove, f"Se ha removido [{str(n_remove)}] tags")

    def get_consignments(self):
        try:
            return Consignments.objects(id=self.consignaciones.id).first()
        except Exception as e:
            print(str(e))
            return None

    def __str__(self):
        return f"({self.id_utr}: {self.utr_nombre})[{len(self.tags)}] tags"

    def to_dict(self):
        return dict(id_utr=self.id_utr, utr_nombre=self.utr_nombre, utr_tipo=self.utr_tipo,
                    tags=[t.to_dict() for t in self.tags], activado=self.activado,
                    utr_code=self.utr_code, protocol=self.protocol,
                    longitude=0 if math.isnan(self.longitude) else self.longitude,
                    latitude=0 if math.isnan(self.latitude) else self.latitude)

    def to_summary(self):
        return dict(id_utr=self.id_utr, utr_nombre=self.utr_nombre, utr_tipo=self.utr_tipo)


class SREntity(EmbeddedDocument):
    id_entidad = StringField(required=True, unique=True, default=None)
    entidad_nombre = StringField(required=True)
    entidad_tipo = StringField(required=True)
    activado = BooleanField(default=True)
    utrs = ListField(EmbeddedDocumentField(SRUTR))

    def __init__(self, *args, **values):
        super().__init__(*args, **values)
        if self.id_entidad is None:
            id = str(self.entidad_nombre).lower().strip() + str(self.entidad_tipo).lower().strip()
            self.id_entidad = hashlib.md5(id.encode()).hexdigest()

    def add_or_replace_utrs(self, utr_list: list):
        # check si todas las utrs son de tipo SRUTR
        check = [isinstance(t, SRUTR) for t in utr_list]
        if not all(check):
            lg = [str(utr_list[i]) for i, v in enumerate(check) if not v]
            return False, [f"La siguiente lista de UTRs no es compatible:"] + lg

        # unificando las lista y crear una sola
        unique = dict()
        unified_list = self.utrs + utr_list
        n_initial = len(self.utrs)
        n_total = len(unified_list)
        for u in unified_list:
            unique.update({u.id_utr: u})
        self.utrs = [unique[k] for k in unique.keys()]
        n_final = len(self.utrs)
        return True, f"UTRs: -remplazadas: [{n_total - n_final}] -añadidas: [{n_final - n_initial}]"

    def add_or_rename_utrs(self, utr_list: list):
        # check si todas las utrs son de tipo SRUTR
        check = [isinstance(t, SRUTR) for t in utr_list]
        if not all(check):
            lg = [str(utr_list[i]) for i, v in enumerate(check) if not v]
            return False, [f"La siguiente lista de UTRs no es compatible:"] + lg
        n_add, n_rename = 0, 0
        for utr in utr_list:
            found = False
            for ix, _utr in enumerate(self.utrs):
                # si existe la UTR actualizar los cambios:
                if _utr.id_utr == utr.id_utr:
                    self.utrs[ix].utr_nombre = utr.utr_nombre
                    self.utrs[ix].utr_tipo = utr.utr_tipo
                    self.utrs[ix].activado = utr.activado
                    self.utrs[ix].protocol = utr.protocol
                    self.utrs[ix].latitude = utr.latitude
                    self.utrs[ix].longitude = utr.longitude
                    found, n_rename = True, n_rename + 1
                    break
            if not found:
                self.utrs.append(utr)
                n_add += 1
        return True, f"UTRs: -editadas: {n_rename}  -añadidas: {n_add}"

    def remove_utrs(self, id_utr_list: list):
        # check si todas las tags son de tipo str
        check = [isinstance(u, str) for u in id_utr_list]
        if not all(check):
            lg = [str(id_utr_list[i]) for i, v in enumerate(check) if not v]
            return False, [f"La siguiente lista de id_utr no es compatible:"] + lg
        n_remove = 0
        for id_utr in id_utr_list:
            new_list = [u for u in self.utrs if u.id_utr != id_utr]
            if len(new_list) != len(self.utrs):
                n_remove += 1
            self.utrs = new_list
        return True, f"Se ha removido [{str(n_remove)}] utrs"

    def __str__(self):
        n_tags = sum([len(u.tags) for u in self.utrs])
        return f"({self.entidad_tipo}) {self.entidad_nombre}: [{str(len(self.utrs))} utrs, {str(n_tags)} tags]"

    def to_dict(self):
        return dict(id_entidad=self.id_entidad, entidad_nombre=self.entidad_nombre, entidad_tipo=self.entidad_tipo,
                    utrs=[u.to_dict() for u in self.utrs], activado=self.activado)

    def to_summary(self):
        n_tags = sum([len(u.tags) for u in self.utrs])
        return dict(id_entidad=self.id_entidad, entidad_nombre=self.entidad_nombre, entidad_tipo=self.entidad_tipo,
                    utrs=len(self.utrs), n_tags=n_tags, activado=self.activado)


class SRNode(Document):
    id_node = StringField(required=True, unique=True, default=None)
    nombre = StringField(required=True)
    tipo = StringField(required=True)
    actualizado = DateTimeField(default=dt.datetime.now())
    entidades = ListField(EmbeddedDocumentField(SREntity))
    activado = BooleanField(default=True)
    document = StringField(required=True, default="SRNode")
    meta = {"collection": "CONFG|Nodos"}

    def __init__(self, *args, **values):
        super().__init__(*args, **values)
        if self.id_node is None:
            id = str(self.nombre).lower().strip() + str(self.tipo).lower().strip() + self.document
            self.id_node = hashlib.md5(id.encode()).hexdigest()

    def add_or_replace_entities(self, entity_list: list):
        # check si todas las entidades son de tipo SREntity
        check = [isinstance(t, SREntity) for t in entity_list]
        if not all(check):
            lg = [str(entity_list[i]) for i, v in enumerate(check) if not v]
            return False, [f"La siguiente lista de entidades no es compatible:"] + lg

        # unificando las lista y crear una sola
        unique = dict()
        unified_list = self.entidades + entity_list
        n_initial = len(self.entidades)
        n_total = len(unified_list)
        for u in unified_list:
            unique.update({u.id_entidad: u})
        self.entidades = [unique[k] for k in unique.keys()]
        n_final = len(self.entidades)
        return True, f"Entidades: -remplazadas: [{n_total - n_final}] -añadidas: [{n_final - n_initial}]"

    def update_summary_info(self, summary: dict):
        # attributos a actualizar:
        attributes_node = ["nombre", "tipo", "activado"]
        attributes_entity = ['entidad_nombre', 'entidad_tipo', 'activado']
        for att in attributes_node:
            self[att] = summary[att]
        id_entities_lcl = [e["id_entidad"] for e in self.entidades]
        id_entities_new = [e["id_entidad"] for e in summary["entidades"]]
        ids_to_delete = [id for id in id_entities_lcl if id not in id_entities_new]
        check = [e["entidad_tipo"] + e["entidad_nombre"] for e in summary["entidades"]]
        # check if there are repeated elements
        if len(check) > len(set(check)):
            return False, "Existen elementos repetidos dentro del nodo"
        # delete those that are not in list coming from user interface
        [self.delete_entity_by_id(id) for id in ids_to_delete]
        # creación de nuevas entidades y actualización de valores
        new_entities = list()
        for i, entity in enumerate(summary["entidades"]):
            if entity["id_entidad"] not in id_entities_lcl:
                e = SREntity(entidad_tipo=entity["entidad_tipo"], entidad_nombre=entity["entidad_nombre"])
            else:
                success, e = self.search_entity_by_id(entity["id_entidad"])
                if not success:
                    continue
            for att in attributes_entity:
                e[att] = summary["entidades"][i][att]
            new_entities.append(e)
        self.entidades = new_entities
        return True, "Todos lo cambios fueron hechos"

    def delete_entity(self, name_delete):
        new_entities = [e for e in self.entidades if name_delete != e.entidad_nombre]
        if len(new_entities) == len(self.entidades):
            return False, f"No existe la entidad [{name_delete}] en el nodo [{self.nombre}]"
        self.entidades = new_entities
        return True, "Entidad eliminada"

    def delete_entity_by_id(self, id_entidad):
        new_entities = [e for e in self.entidades if id_entidad != e.id_entidad]
        if len(new_entities) == len(self.entidades):
            return False, f"No existe la entidad [{id_entidad}] en el nodo [{self.nombre}]"
        self.entidades = new_entities
        return True, "Entidad eliminada"

    def search_entity(self, entidad_nombre: str):
        check = [i for i, e in enumerate(self.entidades) if entidad_nombre == e.entidad_nombre]
        if len(check) > 0:
            return True, self.entidades[check[0]]
        return False, f"No existe entity_list [{entidad_nombre}] en nodo [{self.nombre}]"

    def search_entity_by_id(self, id_entidad: str):
        check = [i for i, e in enumerate(self.entidades) if id_entidad == e.id_entidad]
        if len(check) > 0:
            return True, self.entidades[check[0]]
        return False, f"No existe la entidad [{id_entidad}] en nodo [{self.nombre}]"

    def delete_all(self):
        for e in self.entidades:
            for u in e.utrs:
                try:
                    consignaciones = Consignments.objects(id=u.consignaciones.id)
                    consignaciones.delete()
                except Exception as e:
                    print(str(e))
        self.delete()

    def get_utr_dict(self):
        utrs = dict()
        for entidad in self.entidades:
            for utr in entidad.utrs:
                utrs[utr.id_utr] = utr
        return utrs

    def __str__(self):
        return f"[({self.tipo}) {self.nombre}] entidades: {[str(e) for e in self.entidades]}"

    def to_DataFrame(self):
        cl_activado = "activado"
        cl_utr_name = "utr_nombre"
        cl_utr_type = "utr_tipo"
        cl_entity_name = "entidad_nombre"
        cl_entity_type = "entidad_tipo"
        cl_tag_name = "tag_name"
        cl_f_expression = "filter_expression"
        cl_utr = "utr"
        # main columns in DataFrame
        main_columns = [cl_utr, cl_utr_name, cl_utr_type,
                        cl_entity_name, cl_entity_type, cl_activado]
        # columns in tags sheet
        tags_columns = [cl_utr, cl_tag_name, cl_f_expression, cl_activado]

        df_main = pd.DataFrame(columns=main_columns)
        df_tags = pd.DataFrame(columns=tags_columns)
        for entidad in self.entidades:
            for utr in entidad.utrs:
                active = "x" if utr.activado else ""
                df_main = df_main.append({cl_utr: utr.id_utr, cl_utr_name: utr.utr_nombre,
                                          cl_utr_type: utr.utr_tipo, cl_entity_name: entidad.entidad_nombre,
                                          cl_entity_type: entidad.entidad_tipo, cl_activado: active},
                                         ignore_index=True)
                for tag in utr.tags:
                    active = "x" if tag.activado else ""
                    df_tags = df_tags.append({cl_utr: utr.id_utr, cl_tag_name: tag.tag_name,
                                              cl_f_expression: tag.filter_expression, cl_activado: active},
                                             ignore_index=True)
        return df_main, df_tags

    def to_dict(self):
        return dict(nombre=self.nombre,
                    tipo=self.tipo, entidades=[e.to_dict() for e in self.entidades], actualizado=str(self.actualizado),
                    activado=self.activado)

    def to_summary(self):
        entidades = [e.to_summary() for e in self.entidades]
        n_tags = sum([e["n_tags"] for e in entidades])
        return dict(id_node=self.id_node, nombre=self.nombre,
                    tipo=self.tipo, n_tags=n_tags, entidades=entidades,
                    activado=self.activado, actualizado=str(self.actualizado))


class SRNodeFromDataFrames():
    """
    Clase que permite la conversión de un dataFrame en un nodo de tipo STR
    """

    def __init__(self, tipo, nombre, df_main: pd.DataFrame, df_tags: pd.DataFrame):
        df_main.columns = [str(x).lower() for x in df_main.columns]
        df_tags.columns = [str(x).lower() for x in df_tags.columns]
        self.df_main = df_main
        self.df_tags = df_tags
        self.cl_activado = "activado"
        self.cl_utr_name = "utr_nombre"
        self.cl_utr_type = "utr_tipo"
        self.cl_entity_name = "entidad_nombre"
        self.cl_entity_type = "entidad_tipo"
        self.cl_tag_name = "tag_name"
        self.cl_f_expression = "filter_expression"
        self.cl_utr = "utr"
        self.cl_latitud = "latitud"
        self.cl_longitud = "longitud"
        self.cl_protocolo = "protocolo"
        self.tipo = tipo
        self.nombre = nombre

    def validate(self):
        # check if all columns are present in main sheet
        self.main_columns = [self.cl_utr, self.cl_utr_name, self.cl_utr_type,
                             self.cl_entity_name, self.cl_entity_type,
                             self.cl_latitud, self.cl_longitud, self.cl_protocolo,
                             self.cl_activado]
        check_main = [(str(c) in self.df_main.columns) for c in self.main_columns]
        # check if all columns are, present in tags sheet
        self.tags_columns = [self.cl_utr, self.cl_tag_name, self.cl_f_expression, self.cl_activado]
        check_tags = [(str(c) in self.df_tags.columns) for c in self.tags_columns]

        # incorrect format:
        if not all(check_main):
            to_send = [self.main_columns[i] for i, v in enumerate(check_main) if not v]
            return False, f"La hoja main no contiene los campos: {to_send}. " \
                          f"Los campos necesarios son: [{str(self.main_columns)}]"
        if not all(check_tags):
            to_send = [self.tags_columns[i] for i, v in enumerate(check_tags) if not v]
            return False, f"La hoja tags no contiene los campos: {to_send}. " \
                          f"Los campos necesarios son: [{str(self.tags_columns)}]"

        # if correct then continue with the necessary fields and rows
        self.df_main[self.cl_activado] = [str(a).lower().strip() for a in self.df_main[self.cl_activado]]
        self.df_tags[self.cl_activado] = [str(a).lower().strip() for a in self.df_tags[self.cl_activado]]

        # filter those who are activated
        self.df_main = self.df_main[self.main_columns]
        self.df_tags = self.df_tags[self.tags_columns]
        self.df_main = self.df_main[self.df_main[self.cl_activado] == "x"]
        self.df_tags = self.df_tags[self.df_tags[self.cl_activado] == "x"]

        # if there is spaces after values
        for c in self.main_columns:
            self.df_main[c] = [str(a).strip() for a in self.df_main[c]]
        for c in self.tags_columns:
            self.df_tags[c] = [str(a).strip() for a in self.df_tags[c]]

        return True, f"El formato del nodo [{self.nombre}] es correcto"

    def create_node(self):
        try:
            nodo = SRNode(tipo=self.tipo, nombre=self.nombre)
            df_m = self.df_main.copy().groupby([self.cl_entity_name, self.cl_entity_type])
            df_t = self.df_tags.copy()
            # crear una lista de entidades:
            entities = list()
            for (entity_name, entity_type), df_e in df_m:
                # creando entidad:
                entity = SREntity(entidad_nombre=entity_name, entidad_tipo=entity_type)
                # collección de UTRs
                utrs = list()
                for idx in df_e.index:
                    utr_code = df_e[self.cl_utr].loc[idx]
                    utr_nombre = df_e[self.cl_utr_name].loc[idx]
                    utr_type = df_e[self.cl_utr_type].loc[idx]
                    utr_protocol = df_e[self.cl_protocolo].loc[idx]
                    latitud = float(df_e[self.cl_latitud].loc[idx])
                    longitud = float(df_e[self.cl_longitud].loc[idx])

                    # crear utr para agregar tags
                    utr = SRUTR(id_utr=utr_code, utr_nombre=utr_nombre, utr_tipo=utr_type,
                                protocol=utr_protocol, latitude=latitud, longitude=longitud)
                    # filtrar y añadir tags en la utr list (utrs)
                    df_u = df_t[df_t[self.cl_utr] == utr_code].copy()
                    for ide in df_u.index:
                        tag = SRTag(tag_name=df_u[self.cl_tag_name].loc[ide],
                                    filter_expression=df_u[self.cl_f_expression].loc[ide],
                                    activado=True)
                        # añadir tag en lista de tags
                        utr.tags.append(tag)
                    # añadir utr en lista utr
                    utrs.append(utr)
                # añadir utrs creadas en entidad
                success, msg = entity.add_or_replace_utrs(utrs)
                print(msg) if not success else None
                entities.append(entity)
            # añadir entidades en nodo
            success, msg = nodo.add_or_replace_entities(entities)
            print(msg) if not success else None
            return True, nodo
        except Exception as e:
            print(traceback.format_exc())
            return False, str(e)


class SRDataFramesFromDict():
    """
    Clase que pemite la creación de DataFrames a partir de un dictionary tipo STRNode
    """

    def __init__(self, sr_node_as_dict):
        self.sr_node_as_dict = sr_node_as_dict
        self.cl_activado = "activado"
        self.cl_utr = "utr"
        self.cl_utr_name = "utr_nombre"
        self.cl_utr_type = "utr_tipo"
        self.cl_entity_name = "entidad_nombre"
        self.cl_entity_type = "entidad_tipo"
        self.cl_tag_name = "tag_name"
        self.cl_f_expression = "filter_expression"
        self.cl_utr = "utr"
        self.cl_latitud = "latitud"
        self.cl_longitud = "longitud"
        self.cl_protocolo = "protocolo"

    def convert_to_DataFrames(self):
        # main columns in DataFrame
        main_columns = [self.cl_utr, self.cl_utr_name, self.cl_utr_type,
                        self.cl_entity_name, self.cl_entity_type,
                        self.cl_protocolo, self.cl_longitud, self.cl_latitud, self.cl_activado]
        # columns in tags sheet
        tags_columns = [self.cl_utr, self.cl_tag_name, self.cl_f_expression, self.cl_activado]

        df_main = pd.DataFrame(columns=main_columns)
        df_tags = pd.DataFrame(columns=tags_columns)
        try:
            for entidad in self.sr_node_as_dict["entidades"]:
                for utr in entidad["utrs"]:
                    active = "x" if utr["activado"] else ""
                    df_main = df_main.append({self.cl_utr: utr["id_utr"], self.cl_utr_name: utr["utr_nombre"],
                                              self.cl_utr_type: utr["utr_tipo"],
                                              self.cl_entity_name: entidad["entidad_nombre"],
                                              self.cl_entity_type: entidad["entidad_tipo"],
                                              self.cl_protocolo: utr["protocol"] if "protocol" in utr.keys() else "",
                                              self.cl_longitud: utr["longitude"] if "longitude" in utr.keys() else "",
                                              self.cl_latitud: utr["latitude"] if "latitude" in utr.keys() else "",
                                              self.cl_activado: active}, ignore_index=True)

                    if len(utr["tags"]) == 0:
                        continue
                    _df_tags = pd.DataFrame(utr["tags"])
                    _df_tags[self.cl_utr] = utr["id_utr"]
                    _df_tags[self.cl_activado] = ["x" if t > 0 else "" for t in _df_tags[self.cl_activado]]
                    df_tags = df_tags.append(_df_tags)

            return True, df_main, df_tags, "DataFrame correcto"
        except Exception as e:
            return False, df_main, df_tags, f"No se pudo convertir a DataFrame. Detalle: {str(e)}"
