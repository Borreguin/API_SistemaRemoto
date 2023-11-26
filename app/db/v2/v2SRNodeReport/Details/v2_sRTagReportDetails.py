from mongoengine import EmbeddedDocument, StringField, IntField


class V2SRTagDetails(EmbeddedDocument):
    tag_name = StringField(required=True)
    indisponible_minutos = IntField(required=True)

    def __str__(self):
        return f"{self.tag_name}: {self.indisponible_minutos}"

    def to_dict(self):
        return dict(tag_name=self.tag_name, indisponible_minutos=self.indisponible_minutos)
