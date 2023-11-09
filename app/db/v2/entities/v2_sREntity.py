import hashlib

from mongoengine import EmbeddedDocument, StringField, BooleanField, LazyReferenceField, ListField

from app.db.v2.entities.v2_sRInstallation import V2SRInstallation


class V2SREntity(EmbeddedDocument):
    id_entidad = StringField(required=True, unique=True, default=None, sparse=True)
    entidad_nombre = StringField(required=True)
    entidad_tipo = StringField(required=True)
    activado = BooleanField(default=True)
    installations = ListField(LazyReferenceField(V2SRInstallation))

    def __init__(self, entidad_tipo: str = None, entidad_nombre: str = None, *args, **values):
        super().__init__(*args, **values)
        if entidad_tipo is not None:
            self.entidad_tipo = entidad_tipo
        if entidad_nombre is not None:
            self.entidad_nombre = entidad_nombre
        if self.id_entidad is None:
            id = str(self.entidad_nombre).lower().strip() + str(self.entidad_tipo).lower().strip()
            self.id_entidad = hashlib.md5(id.encode()).hexdigest()

    def __str__(self):
        return f"({self.entidad_tipo}) {self.entidad_nombre}: [{str(len(self.installations))} installations]"
