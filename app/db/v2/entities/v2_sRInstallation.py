import datetime as dt

from mongoengine import Document, StringField, ReferenceField, BooleanField, FloatField, ListField, \
    EmbeddedDocumentField, DateTimeField, NotUniqueError

from app.db.constants import SR_INSTALLATION_COLLECTION
from app.db.v1.Info.Consignment import Consignments
from app.db.v2.entities.v2_sRBahia import V2SRBahia


class V2SRInstallation(Document):
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

    def __str__(self):
        return f"({self.instalacion_tipo}) {self.instalacion_nombre}: [{str(len(self.bahias))} bahias]"

    def save_safely(self, *args, **kwargs):
        try:
            super().save(*args, **kwargs)
            return True, f"SRInstallationV2: Saved successfully"
        except NotUniqueError:
            return False, f"SRInstallationV2: no único para valores: {self.instalacion_ems_code}"
        except Exception as e:
            return False, f"No able to save: {e}"
