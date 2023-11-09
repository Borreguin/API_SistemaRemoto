from mongoengine import EmbeddedDocument, StringField, BooleanField, LazyReferenceField, ListField

from app.db.v2.entities.v2_sRInstallation import V2SRInstallation


class V2SREntity(EmbeddedDocument):
    id_entidad = StringField(required=True, unique=True, default=None)
    entidad_nombre = StringField(required=True)
    entidad_tipo = StringField(required=True)
    activado = BooleanField(default=True)
    installations = ListField(LazyReferenceField(V2SRInstallation))
