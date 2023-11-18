import hashlib

from mongoengine import EmbeddedDocument, StringField, BooleanField, LazyReferenceField, ListField

from app.db.constants import lb_n_tags, lb_n_bahias
from app.db.v2.entities.v2_sRInstallation import V2SRInstallation


class V2SREntity(EmbeddedDocument):
    id_entidad = StringField(required=True, unique=True, default=None, sparse=True)
    entidad_nombre = StringField(required=True)
    entidad_tipo = StringField(required=True)
    activado = BooleanField(default=True)
    instalaciones = ListField(LazyReferenceField(V2SRInstallation))

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
        return f"({self.entidad_tipo}) {self.entidad_nombre}: [{str(len(self.instalaciones))} instalaciones]"

    def to_summary(self):
        n_tags = 0
        n_bahias = 0
        if self.instalaciones is not None:
            for instalacion in self.instalaciones:
                values_dict = instalacion.fetch().to_summary()
                n_tags += values_dict[lb_n_tags]
                n_bahias += values_dict[lb_n_bahias]

        return dict(id_entidad=self.id_entidad, entidad_nombre=self.entidad_nombre, entidad_tipo=self.entidad_tipo,
                    n_instalaciones=len(self.instalaciones) if self.instalaciones is not None else 0, n_bahias=n_bahias,
                    n_tags=n_tags)
