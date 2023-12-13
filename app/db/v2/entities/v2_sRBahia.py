from __future__ import annotations
import uuid

from mongoengine import EmbeddedDocument, StringField, FloatField, BooleanField, ListField, EmbeddedDocumentField, \
    DateTimeField


from app.db.v2.entities.v2_sRTag import V2SRTag
import datetime as dt

from app.utils.excel_util import convert_to_float


class V2SRBahia(EmbeddedDocument):
    bahia_code = StringField(required=True, sparse=True, default=None)
    voltaje = FloatField(required=False, default=None)
    bahia_nombre = StringField(required=True)
    tags = ListField(EmbeddedDocumentField(V2SRTag))
    activado = BooleanField(default=True)
    document_id = StringField(required=True, default=None)
    created = DateTimeField(default=dt.datetime.now())

    def __init__(self, bahia_code: str = None, bahia_nombre: str = None, voltaje: str | float = None, *args, **values):
        super().__init__(*args, **values)
        if bahia_code is not None:
            self.bahia_code = bahia_code
        if voltaje is not None:
            self.voltaje = convert_to_float(voltaje)
        if bahia_nombre is not None:
            self.bahia_nombre = bahia_nombre
        if self.document_id is not None:
            self.document_id = str(uuid.uuid4())

    def __str__(self):
        return f"{self.bahia_nombre}: ({self.voltaje} kV) nTags: {len(self.tags) if self.tags is not None else 0}"

    def to_dict(self):
        return dict(bahia_code=self.bahia_code, bahia_nombre=self.bahia_nombre, voltaje=self.voltaje,
                    tags=[t.to_dict() for t in self.tags] if self.tags is not None else [],
                    document_id=self.document_id)

    def to_summary(self):
        return dict(bahia_code=self.bahia_code, bahia_nombre=self.bahia_nombre, voltaje=self.voltaje,
                    n_tags=len(self.tags) if self.tags is not None else 0, document_id=self.document_id)

    def get_document_id(self):
        return str(self.document_id)