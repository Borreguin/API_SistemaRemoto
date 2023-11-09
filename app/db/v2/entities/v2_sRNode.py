from mongoengine import Document, StringField, DateTimeField, ListField, BooleanField, EmbeddedDocumentField
import datetime as dt

from app.db.constants import V2_SR_NODE_LABEL, SR_NODE_COLLECTION
from app.db.v2.entities.v2_sREntity import V2SREntity


class V2SRNode(Document):
    id_node = StringField(required=True, unique=True, default=None)
    nombre = StringField(required=True)
    tipo = StringField(required=True)
    actualizado = DateTimeField(default=dt.datetime.now())
    entidades = ListField(EmbeddedDocumentField(V2SREntity))
    activado = BooleanField(default=True)
    document = StringField(required=True, default=V2_SR_NODE_LABEL)
    meta = {"collection": SR_NODE_COLLECTION}
