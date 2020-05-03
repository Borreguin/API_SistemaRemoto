# Created by Roberto Sanchez at 21/04/2020
# -*- coding: utf-8 -*-

"""
    Clases que relacionan los documentos JSON de la base de datos de MongoDB con los objetos creados
    Object mapper for SRNodes

"""
import hashlib

from mongoengine import *
from my_lib.mongo_engine_handler.Consignment import *
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


class SREntity(EmbeddedDocument):
    id_entidad = StringField(required=True, unique=True)
    nombre = StringField(required=True)
    tipo = StringField(required=True)
    tags = ListField(EmbeddedDocumentField(SRTag))
    activado = BooleanField(default=True)
    consignaciones = LazyReferenceField(Consignments, dbref=True, passthrough=True)

    def __init__(self, *args, **values):
        super().__init__(*args, **values)
        # check if there are consignaciones related with id_entidad
        consignaciones = Consignments.objects(id_entidad=self.id_entidad).first()
        if consignaciones is None:
            # if there are not consignaciones then create a new document
            consignaciones = Consignments(id_entidad=self.id_entidad).save()
        # relate an existing consignacion
        self.consignaciones = consignaciones

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
        final_list = [unique_tags[k] for k in unique_tags.keys()]
        self.tags = final_list
        return True, "Insertada las tags de manera correcta"

    def remove_tags(self, tag_list:list):
        # check si todas las tags son de tipo str
        check_tags = [isinstance(t, str) for t in tag_list]
        if not all(check_tags):
            lg = [str(tag_list[i]) for i, v in enumerate(check_tags) if not v]
            return False, [f"La siguiente lista de tags no es compatible:"] + lg
        n_remove = 0
        for tag in tag_list:
            new_list = [t for t in self.tags if t.tag_name != tag]
            if len(new_list) != len(self.tags):
                n_remove += 1
            self.tags = new_list
        return True, f"Se ha removido [{str(n_remove)}] tags"

    def get_consignments(self):
        try:
            return Consignments.objects(id=self.consignaciones.id).first()
        except Exception as e:
            print(str(e))
            return None

    def __str__(self):
        return f"({self.id_entidad}) {self.nombre}: [{str(len(self.tags))}] tags"

    def to_dict(self, *args, **kwargs):
        return dict(id_entidad=self.id_entidad, nombre=self.nombre, tipo=self.tipo,
                    tags=[t.to_dict() for t in self.tags])


class SRNode(Document):
    id_node = StringField(required=True, unique=True)
    nombre = StringField(required=True)
    tipo = StringField(required=True)
    actualizado = DateTimeField(default=dt.datetime.now())
    entidades = ListField(EmbeddedDocumentField(SREntity))
    activado = BooleanField(default=True)
    meta = {"collection": "CONFG|Nodos"}

    def __init__(self, *args, **values):
        super().__init__(*args, **values)
        id = str(self.nombre).lower().strip() + str(self.tipo).lower().strip()
        self.id_node = hashlib.md5(id.encode()).hexdigest()

    def add_or_replace_entidad(self, entidad: SREntity):
        check = [i for i, e in enumerate(self.entidades) if entidad.id_entidad == e.id_entidad]
        if len(check) > 0:
            self.entidades[check[0]] = entidad
        else:
            self.entidades.append(entidad)

    def delete_entity(self, name_delete):
        new_entities = [e for e in self.entidades if name_delete != e.nombre]
        if len(new_entities) == len(self.entidades):
            return False, f"No existe la entidad [{name_delete}] en el nodo [{self.nombre}]"
        self.entidades = new_entities
        return True, "Entidad eliminada"

    def search_this(self, nombre_entidad: str):
        check = [i for i, e in enumerate(self.entidades) if nombre_entidad == e.nombre]
        if len(check) > 0:
            return True, self.entidades[check[0]]
        return False, f"No existe entidad [{nombre_entidad}] en nodo [{self.nombre}]"

    def add_or_replace_tags_in_entity(self, tag_list: list, nombre_entidad: str):
        check = [i for i, e in enumerate(self.entidades) if nombre_entidad == e.nombre]
        if len(check) == 0:
            return False, f"No existe entidad [{nombre_entidad}] en nodo [{self.nombre}]"
        return self.entidades[check[0]].add_or_replace_tags(tag_list)

    def remove_tags_in_entity(self, tag_list: list, nombre_entidad: str):
        check = [i for i, e in enumerate(self.entidades) if nombre_entidad == e.nombre]
        if len(check) == 0:
            return False, f"No existe entidad [{nombre_entidad}] en nodo [{self.nombre}]"
        return self.entidades[check[0]].remove_tags(tag_list)

    def delete_all(self):
        for e in self.entidades:
            try:
                consignaciones = Consignments.objects(id=e.consignaciones.id)
                consignaciones.delete()
            except Exception as e:
                print(str(e))
        self.delete()

    def __str__(self):
        return f"[({self.tipo}) {self.nombre}] entidades: {[str(e) for e in self.entidades]}"

    def to_dict(self, *args, **kwargs):
        return dict(nombre=self.nombre,
            tipo=self.tipo, entidades=[e.to_dict() for e in self.entidades], actualizado=str(self.actualizado))


class SRNodeFromDataFrames():

    def __init__(self, nombre, tipo, df_main: pd.DataFrame, df_tags: pd.DataFrame):
        df_main.columns = [str(x).lower() for x in df_main.columns]
        df_tags.columns = [str(x).lower() for x in df_tags.columns]
        self.df_main = df_main
        self.df_tags = df_tags
        self.cl_activado = "activado"
        self.cl_name = "nombre"
        self.cl_entidades = "utr"
        self.cl_type = "tipo"
        self.cl_tag_name = "tag_name"
        self.cl_f_expression = "filter_expression"
        self.cl_utr = "utr"
        self.nombre = nombre
        self.tipo = tipo

    def validate(self):
        # check if all columns are present in main sheet
        self.main_columns = [self.cl_entidades, self.cl_name, self.cl_type, self.cl_activado]
        check_main = [(str(c) in self.df_main.columns) for c in self.main_columns]
        # check if all columns are present in tags sheet
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
        self.df_main[self.cl_activado] = [str(a).lower() for a in self.df_main[self.cl_activado]]
        self.df_tags[self.cl_activado] = [str(a).lower() for a in self.df_tags[self.cl_activado]]

        # filter those who are activated
        self.df_main = self.df_main[self.main_columns]
        self.df_tags = self.df_tags[self.tags_columns]
        self.df_main = self.df_main[self.df_main[self.cl_activado] == "x"]
        self.df_tags = self.df_tags[self.df_tags[self.cl_activado] == "x"]
        return True, f"El formato del nodo [{self.nombre}] es correcto"

    def create_node(self):
        try:
            nodo = SRNode(nombre=self.nombre, tipo=self.tipo)
            df_m = self.df_main.copy()
            df_t = self.df_tags.copy()
            for idx in df_m.index:
                # crear entidad
                id_entidad = df_m[self.cl_entidades].loc[idx]
                entidad = SREntity(id_entidad=id_entidad,
                                   nombre=df_m[self.cl_name].loc[idx],
                                   tipo=df_m[self.cl_type].loc[idx])
                df_e = df_t[df_t[self.cl_utr] == id_entidad].copy()
                # añadir tags en entidad
                for ide in df_e.index:
                    tag = SRTag(tag_name=df_e[self.cl_tag_name].loc[ide],
                                filter_expression=df_e[self.cl_f_expression].loc[ide],
                                activado=True)
                    entidad.tags.append(tag)
                # añadir entidad en nodo
                nodo.entidades.append(entidad)
            return True, nodo
        except Exception as e:
            return False, str(e)