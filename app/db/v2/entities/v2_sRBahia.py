from mongoengine import EmbeddedDocument, StringField, FloatField, BooleanField, ListField, EmbeddedDocumentField

from app.db.v2.entities.v2_sRTag import V2SRTag


class V2SRBahia(EmbeddedDocument):
    bahia_code = StringField(required=True, sparse=True, default=None)
    voltaje = FloatField(required=False, default=None)
    bahia_nombre = StringField(required=True)
    tags = ListField(EmbeddedDocumentField(V2SRTag))
    activado = BooleanField(default=True)

    def __init__(self, bahia_code: str = None, voltaje: str = None, bahia_nombre: str = None, *args, **values):
        super().__init__(*args, **values)
        if bahia_code is not None:
            self.bahia_code = bahia_code
        if voltaje is not None:
            self.voltaje = voltaje
        if bahia_nombre is not None:
            self.bahia_nombre = bahia_nombre

    def __str__(self):
        return f"{self.bahia_nombre}: ({self.voltaje} kV) nTags: {len(self.tags) if self.tags is not None else 0}"
