from mongoengine import EmbeddedDocument, StringField, DictField


class V2SRConsignmentDetails(EmbeddedDocument):
    description = StringField(required=False, default=None)
    short_description = StringField(required=False, default=None)
    consignment_type = StringField(required=False, default=None)
    element_info = DictField(required=False, default=None)