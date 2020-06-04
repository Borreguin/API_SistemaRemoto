from mongoengine import *
import datetime as dt
import hashlib


class DiagramSerializer(Document):
    id_diagram = StringField(required=True, unique=True)
    nombre = StringField(required=True)
    tipo = StringField(required=True)
    diagrama = DictField(required=True)
    actualizado = DateTimeField(default=dt.datetime.now())
    meta = {"collection": "UI|Diagrams"}

    def __init__(self, *args, **values):
        super().__init__(*args, **values)
        id = str(self.nombre).lower().strip() + str(self.tipo).lower().strip()
        self.id_node = hashlib.md5(id.encode()).hexdigest()
        self.actualizado = DateTimeField(default=dt.datetime.now())
