import hashlib

from mongoengine import Document, StringField, DateTimeField, ListField, BooleanField, EmbeddedDocumentField, \
    NotUniqueError
import datetime as dt

from app.db.constants import V2_SR_NODE_LABEL, SR_NODE_COLLECTION
from app.db.v2.entities.v2_sREntity import V2SREntity


class V2SRNode(Document):
    id_node = StringField(required=True, unique=True, default=None)
    nombre = StringField(required=True)
    tipo = StringField(required=True)
    actualizado = DateTimeField(default=dt.datetime.now())
    entidades = ListField(EmbeddedDocumentField(V2SREntity), default=None)
    activado = BooleanField(default=True)
    document = StringField(required=True, default=V2_SR_NODE_LABEL)
    meta = {"collection": SR_NODE_COLLECTION}

    def __init__(self, tipo: str = None, nombre: str = None, *args, **values):
        super().__init__(*args, **values)
        if tipo is not None:
            self.tipo = tipo
        if nombre is not None:
            self.nombre = nombre

        if self.id_node is None:
            id = str(self.nombre).lower().strip() + str(self.tipo).lower().strip() + self.document
            self.id_node = hashlib.md5(id.encode()).hexdigest()

    def __str__(self):
        return (f"[({self.tipo}) {self.nombre}] "
                f"entidades: {[str(e) for e in self.entidades] if self.entidades is not None else 0}")

    def save_safely(self, *args, **kwargs):
        try:
            super().save(*args, **kwargs)
            return True, f"SRNodeV2: Saved successfully"
        except NotUniqueError:
            return False, f"SRNodeV2: no único para valores: {self.tipo} {self.nombre}"
        except Exception as e:
            return False, f"No able to save: {e}"

    @staticmethod
    def find(tipo: str, nombre: str):
        return V2SRNode.objects(tipo=tipo, nombre=nombre, document=V2_SR_NODE_LABEL).first()


