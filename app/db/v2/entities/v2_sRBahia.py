from mongoengine import EmbeddedDocument, StringField, FloatField, BooleanField


class V2SRBahia(EmbeddedDocument):
    bahia_code = StringField(required=True, sparse=True, default=None)
    voltaje = FloatField(required=False, default=None)
    bahia_nombre = StringField(required=True)
    activado = BooleanField(default=True)

    def __init__(self, bahia_code: str = None, voltaje: str = None, bahia_nombre: str = None, *args, **values):
        super().__init__(*args, **values)
        if bahia_code is not None:
            self.bahia_code = bahia_code
        if voltaje is not None:
            self.voltaje = voltaje
        if bahia_nombre is not None:
            self.bahia_nombre = bahia_nombre


