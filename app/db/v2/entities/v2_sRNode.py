from __future__ import annotations
import datetime as dt
import hashlib
import traceback

import pandas as pd
from mongoengine import Document, StringField, DateTimeField, ListField, BooleanField, EmbeddedDocumentField, IntField

from app.common import error_log
from app.db.constants import V2_SR_NODE_LABEL, SR_NODE_COLLECTION, lb_n_bahias, lb_n_tags, lb_n_instalaciones, \
    lb_n_entidades
from app.db.v2.entities.v2_sREntity import V2SREntity
from app.db.v2.v2_util import get_or_replace_entities_and_installations_from_dataframe, \
    get_or_replace_bahias_and_tags_from_dataframe


class V2SRNode(Document):
    id_node = StringField(required=True, unique=True, default=None)
    nombre = StringField(required=True)
    tipo = StringField(required=True)
    actualizado = DateTimeField(default=dt.datetime.now())
    entidades = ListField(EmbeddedDocumentField(V2SREntity), default=[])
    activado = BooleanField(default=True)
    document = StringField(required=True, default=V2_SR_NODE_LABEL)
    n_tags = IntField(default=0)
    n_bahias = IntField(default=0)
    n_instalaciones = IntField(default=0)
    n_entidades = IntField(default=0)
    meta = {"collection": SR_NODE_COLLECTION}

    def __init__(self, tipo: str = None, nombre: str = None, *args, **values):
        super().__init__(*args, **values)
        if tipo is not None:
            self.tipo = tipo
        if nombre is not None:
            self.nombre = nombre

        if self.id_node is None:
            self.update_node_id()

    def update_node_id(self):
        id = str(self.nombre).lower().strip() + str(self.tipo).lower().strip() + self.document
        self.id_node = hashlib.md5(id.encode()).hexdigest()

    def update_summary(self):
        summary = self.to_summary()
        self.n_tags = summary[lb_n_tags]
        self.n_bahias = summary[lb_n_bahias]
        self.n_instalaciones = summary[lb_n_instalaciones]
        self.n_entidades = summary[lb_n_entidades]
        [e.update_summary() for e in self.entidades if self.entidades is not None]

    def __str__(self):
        return (f"v2SRNode [({self.tipo}) {self.nombre}] "
                f"entidades: {[str(e) for e in self.entidades] if self.entidades is not None else 0}")

    def to_dict(self):
        return dict(_id=str(self.pk), id_node=self.id_node, nombre=self.nombre, tipo=self.tipo, actualizado=self.actualizado,
                    entidades=[e.to_dict() for e in self.entidades] if self.entidades is not None else [],
                    activado=self.activado, n_tags=self.n_tags, n_bahias=self.n_bahias, n_instalaciones=self.n_instalaciones)

    def get_document_id(self):
        return str(self.pk)

    def to_summary(self):
        n_entidades, n_instalaciones, n_bahias, n_tags = 0, 0, 0, 0
        entidades = list()
        for entidad in self.entidades if self.entidades is not None else []:
            values = entidad.to_summary()
            n_entidades += 1
            n_instalaciones += values[lb_n_instalaciones]
            n_bahias += values[lb_n_bahias]
            n_tags += values[lb_n_tags]
            entidades.append(values)
        return dict(_id=str(self.pk), document= V2_SR_NODE_LABEL,id_node=self.id_node, nombre=self.nombre,
                    tipo=self.tipo, entidades=entidades,
                    actualizado=self.actualizado, activado=self.activado, n_entidades=n_entidades,
                    n_instalaciones=n_instalaciones, n_bahias=n_bahias, n_tags=n_tags)

    def save(self, *args, **kwargs):
        self.update_node_id()
        self.update_summary()
        return super().save(*args, **kwargs)

    def save_safely(self, *args, **kwargs):
        from app.db.db_util import save_mongo_document_safely
        self.update_node_id()
        self.update_summary()
        return save_mongo_document_safely(self, *args, **kwargs)

    def delete_deeply(self, *args, **kwargs):
        if self.entidades is not None:
            for entidad in self.entidades:
                if entidad.instalaciones is not None:
                    for instalacion in entidad.instalaciones:
                        instalacion.fetch().delete()
        self.delete(*args, **kwargs)

    def save_deeply(self, *args, **kwargs):
        try:
            if self.entidades is not None:
                for entidad in self.entidades:
                    entidad.update_entity_id()
                    if entidad.instalaciones is not None:
                        for instalacion in entidad.instalaciones:
                            instalacion.fetch().save_safely(*args, **kwargs)
            return self.save_safely(*args, **kwargs)
        except Exception as e:
            return False, f"No able to save: {e}"

    @staticmethod
    def find(tipo: str, nombre: str):
        nodes = V2SRNode.objects(tipo=tipo, nombre=nombre, document=V2_SR_NODE_LABEL)
        return nodes.first() if len(nodes) > 0 else None

    @staticmethod
    def find_or_create_if_not_exists_node(tipo: str, nombre: str, create_if_not_exists=False) -> tuple[
        bool, str, 'V2SRNode']:
        node = V2SRNode.find(tipo, nombre)
        if node is None and create_if_not_exists:
            node = V2SRNode(tipo, nombre)
            success, msg = node.save_safely()
            if not success:
                return False, msg, node
            return True, 'Node created', node
        return True, 'Node found', node

    def replace_or_edit_new_entities(self,
                                     df_bahia: pd.DataFrame,
                                     df_tags: pd.DataFrame,
                                     replace: bool,
                                     edit:bool) -> tuple[list[V2SREntity], str]:
        new_entidades = []
        msg = ""
        for entidad in self.entidades:
            new_instalaciones = []
            for instalacion in entidad.instalaciones:
                instalacion = instalacion.fetch()
                success_bahias, partially_success, msg_bahia, new_installation = (
                    get_or_replace_bahias_and_tags_from_dataframe(
                        instalacion, df_bahia, df_tags, replace=replace,edit=edit)
                )
                success_save, msg_save = new_installation.save_safely()
                if (success_bahias or partially_success) and success_save:
                    new_instalaciones.append(new_installation)
                else:
                    msg += f"\n{msg_bahia}\n{msg_save}"
            entidad.instalaciones = new_instalaciones
            new_entidades.append(entidad)
        return new_entidades, msg

    def create_or_edit_node_from_dataframes(self, df_main: pd.DataFrame, df_bahia: pd.DataFrame, df_tags: pd.DataFrame,
                                            replace=False, edit=False) -> tuple[bool, str, 'V2SRNode']:
        msg = f"Node created from dataframes"
        try:
            (success_entities, msg_entities, self.entidades) = (
                get_or_replace_entities_and_installations_from_dataframe(df_main, replace=replace)
            )
            if not success_entities:
                msg += f"\n{msg_entities}"

            self.entidades, entities_msg = self.replace_or_edit_new_entities(df_bahia, df_tags, replace, edit)
            msg += f"\n{entities_msg}" if entities_msg != "" else ""
            success = True
        except Exception as e:
            msg = f"No able to create node from dataframes: {e}"
            tb = traceback.format_exc()
            error_log.error(msg)
            error_log.error(tb)
            success = False
        return success, msg, self

    def delete_entity_by_id(self, id_entidad):
        new_entities = [e for e in self.entidades if self.entidades is not None and id_entidad != e.id_entidad]
        if len(new_entities) == len(self.entidades):
            return False, f"No existe la entidad [{id_entidad}] en el nodo [{self.nombre}]"
        self.entidades = new_entities
        return True, "Entidad eliminada"


    def search_entity_by_id(self, id_entidad: str) -> tuple[bool, str, V2SREntity | None]:
        check = [i for i, e in enumerate(self.entidades) if id_entidad == e.id_entidad]
        if len(check) > 0:
            return True, "Entidad encontrada" , self.entidades[check[0]]
        return False, f"No existe la entidad [{id_entidad}] en nodo [{self.nombre}]", None

    def replace_entity_by_id(self, id_entidad: str, new_entity: V2SREntity) -> tuple[bool, str]:
        check = [i for i, e in enumerate(self.entidades) if id_entidad == e.id_entidad]
        if len(check) > 0:
            self.entidades[check[0]] = new_entity
            return True, "Entidad reemplazada"
        return False, f"No existe la entidad [{id_entidad}] en nodo [{self.nombre}]"
