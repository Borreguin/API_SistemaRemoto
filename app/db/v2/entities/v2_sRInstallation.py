from mongoengine import Document, StringField


class V2SRInstallation(Document):
    id_node = StringField(required=True, unique=True, default=None)
    nombre = StringField(required=True)