from mongoengine import EmbeddedDocument, StringField, BooleanField, DateTimeField

from app.utils.excel_constants import default_filter_expression
import datetime as dt

class V2SRTag(EmbeddedDocument):
    tag_name = StringField(required=True)
    filter_expression = StringField(required=True, default=default_filter_expression)
    activado = BooleanField(default=True)
    created = DateTimeField(default=dt.datetime.now())

    def __init__(self, tag_name: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if tag_name is not None:
            self.tag_name = tag_name

    def __str__(self):
        return f"{self.tag_name}: {self.activado}"

    def to_dict(self):
        return dict(tag_name=self.tag_name, filter_expression=self.filter_expression, activado=self.activado)
