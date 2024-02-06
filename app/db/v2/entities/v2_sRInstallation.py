import datetime as dt
import hashlib
import math

from mongoengine import Document, StringField, ReferenceField, BooleanField, FloatField, ListField, \
    EmbeddedDocumentField, DateTimeField, NotUniqueError

from app.db.constants import SR_INSTALLATION_COLLECTION, V2_SR_INSTALLATION_LABEL, attributes_editable_installation
from app.db.v1.Info.Consignment import Consignments
from app.db.v2.entities.v2_sRBahia import V2SRBahia


class V2SRInstallation(Document):
    instalacion_id = StringField(required=True, unique=True, default=None)
    instalacion_ems_code = StringField(required=True, unique=True, default=None)
    instalacion_nombre = StringField(required=True)
    instalacion_tipo = StringField(required=True)
    activado = BooleanField(default=True)
    protocolo = StringField(default="No definido", required=False)
    longitud = FloatField(required=False, default=0)
    latitud = FloatField(required=False, default=0)
    bahias = ListField(EmbeddedDocumentField(V2SRBahia))
    actualizado = DateTimeField(default=dt.datetime.now())
    document = StringField(required=True, default=V2_SR_INSTALLATION_LABEL)
    document_id = StringField(required=False, default=None)
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
            self.instalacion_id = self.generate_instalacion_id()
        if self.document_id is None:
            self.document_id = self.generate_instalacion_id()

    def __str__(self):
        return f"({self.instalacion_tipo}) {self.instalacion_nombre}: [{str(len(self.bahias))} bahias]"

    def generate_instalacion_id(self):
        id = self.instalacion_ems_code.lower() if self.instalacion_ems_code is not None \
                else self.instalacion_tipo.lower() + self.instalacion_nombre.lower()
        return hashlib.md5(id.encode()).hexdigest()

    def to_dict(self):
        return dict(_id=str(self.pk), instalacion_id=self.instalacion_id, document_id=self.get_document_id(),
                    instalacion_ems_code=self.instalacion_ems_code,
                    instalacion_nombre=self.instalacion_nombre, instalacion_tipo=self.instalacion_tipo,
                    activado=self.activado, protocolo=self.protocolo,
                    longitud=0 if math.isnan(self.longitud) else self.longitud,
                    latitud=0 if math.isnan(self.latitud) else self.latitud,
                    bahias=[b.to_dict() for b in self.bahias] if self.bahias is not None else [])

    def to_summary(self):
        n_tags = 0
        if self.bahias is not None:
            for bahia in self.bahias:
                n_tags += len(bahia.tags) if bahia.tags is not None else 0
        return dict(_id=str(self.pk), instalacion_id=self.instalacion_id, instalacion_ems_code=self.instalacion_ems_code,
                    instalacion_nombre=self.instalacion_nombre, instalacion_tipo=self.instalacion_tipo,
                    n_bahias=len(self.bahias) if self.bahias is not None else 0, n_tags=n_tags,
                    document_id=self.get_document_id())

    def get_document_id(self):
        if self.document_id is None:
            self.document_id = self.generate_instalacion_id()
            self.save_safely()
        return self.document_id

    def save_safely(self, *args, **kwargs):
        from app.db.db_util import save_mongo_document_safely
        return save_mongo_document_safely(self)

    @staticmethod
    def find_by_ems_code(instalacion_ems_code: str) -> 'V2SRInstallation':
        instalacion = V2SRInstallation.objects(instalacion_ems_code=instalacion_ems_code)
        return instalacion.first() if len(instalacion) > 0 else None

    def update_from_dict(self, values:dict):
        for attribute in attributes_editable_installation:
            if attribute in values.keys():
                setattr(self, attribute, values[attribute])

    def find_bahia_by_id(self, document_id:str):
        for bahia in self.bahias:
            if bahia.document_id == document_id:
                return bahia
        return None

    def add_bahia(self, bahia: V2SRBahia):
        if any([True for b in self.bahias if b.document_id == bahia.document_id or
                                             (str(b.bahia_code).lower() == str(bahia.bahia_code).lower() and b.voltaje == bahia.voltaje)]):
            return False, 'La bahia ya existe'
        self.bahias.append(bahia)
        self.save_safely()
        return True, 'Bahia agregada'

    def remove_bahia(self, bahia: V2SRBahia):
        original_len = len(self.bahias)
        self.bahias = [b for b in self.bahias if b.document_id != bahia.document_id]
        deleted = original_len != len(self.bahias)
        if deleted:
            self.save_safely()
        return deleted, 'Bahia eliminada' if deleted else 'Bahia no encontrada'

    def update_bahia(self, bahia_id:str, bahia: V2SRBahia):
        for i in range(len(self.bahias)):
            if self.bahias[i].document_id == bahia_id:
                bahia.document_id = bahia_id
                self.bahias[i] = bahia
                self.save_safely()
                return True, 'Bahia actualizada'
        return False, 'Bahia no encontrada'
