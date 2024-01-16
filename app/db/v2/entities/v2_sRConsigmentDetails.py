from mongoengine import EmbeddedDocument, StringField, DictField


class V2SRConsignmentDetails(EmbeddedDocument):
    detalle = StringField(required=False, default=None)
    descripcion_corta = StringField(required=False, default=None)
    consignment_type = StringField(required=False, default=None)
    element = DictField(required=False, default=None)

    def to_dict(self):
        return dict(detalle=self.detalle, descripcion_corta=self.descripcion_corta,
                    consignment_type=self.consignment_type, element=self.element)