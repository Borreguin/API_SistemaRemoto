import datetime as dt
import hashlib
import math

from mongoengine import Document, StringField, ReferenceField, BooleanField, FloatField, ListField, \
    EmbeddedDocumentField, DateTimeField, NotUniqueError

from app.db.constants import SR_INSTALLATION_COLLECTION
from app.db.v1.Info.Consignment import Consignments
from app.db.v2.entities.v2_sRBahia import V2SRBahia


class V2SRInstallation(Document):
    instalacion_id = StringField(required=True, unique=True, default=None)
    instalacion_ems_code = StringField(required=True, unique=True, default=None)
    instalacion_nombre = StringField(required=True)
    instalacion_tipo = StringField(required=True)
    consignaciones = ReferenceField(Consignments, dbref=True)
    activado = BooleanField(default=True)
    protocolo = StringField(default="No definido", required=False)
    longitud = FloatField(required=False, default=0)
    latitud = FloatField(required=False, default=0)
    bahias = ListField(EmbeddedDocumentField(V2SRBahia))
    actualizado = DateTimeField(default=dt.datetime.now())
    document = StringField(required=True, default="SRInstallationV2")
    meta = {"collection": SR_INSTALLATION_COLLECTION}

    def __init__(self, instalacion_ems_code: str = None, instalacion_tipo: str = None, instalacion_nombre: str = None,
                 *args, **values):
        super().__init__(*args, **values)
        if instalacion_tipo is not None:
            self.instalacion_tipo = instalacion_tipo
        if instalacion_nombre is not None:
            self.instalacion_nombre = instalacion_nombre
        if instalacion_ems_code is not None:
            self.instalacion_ems_code = instalacion_ems_code
        if self.instalacion_id is None:
            id = self.instalacion_ems_code if self.instalacion_ems_code is not None \
                else self.instalacion_tipo + self.instalacion_nombre
            self.instalacion_id = hashlib.md5(id.encode()).hexdigest()

    def __str__(self):
        return f"({self.instalacion_tipo}) {self.instalacion_nombre}: [{str(len(self.bahias))} bahias]"

    def to_dict(self):
        return dict(_id=str(self.pk),instalacion_id=self.instalacion_id, instalacion_ems_code=self.instalacion_ems_code,
                    instalacion_nombre=self.instalacion_nombre, instalacion_tipo=self.instalacion_tipo,
                    consignaciones=self.consignaciones.id if self.consignaciones is not None else None,
                    activado=self.activado, protocolo=self.protocolo,
                    longitud=0 if math.isnan(self.longitud) else self.longitud,
                    latitud=0 if math.isnan(self.latitud) else self.latitud,
                    bahias=[b.to_dict() for b in self.bahias] if self.bahias is not None else [])

    def to_summary(self):
        n_tags = 0
        if self.bahias is not None:
            for bahia in self.bahias:
                n_tags += len(bahia.tags) if bahia.tags is not None else 0
        return dict(instalacion_id=self.instalacion_id, instalacion_ems_code=self.instalacion_ems_code,
                    instalacion_nombre=self.instalacion_nombre, instalacion_tipo=self.instalacion_tipo,
                    n_bahias=len(self.bahias) if self.bahias is not None else 0, n_tags=n_tags)

    def save_safely(self, *args, **kwargs):
        from app.db.util import save_mongo_document_safely
        return save_mongo_document_safely(self)

    @staticmethod
    def find_by_ems_code(instalacion_ems_code: str) -> 'V2SRInstallation':
        instalacion = V2SRInstallation.objects(instalacion_ems_code=instalacion_ems_code)
        return instalacion.first() if len(instalacion) > 0 else None

