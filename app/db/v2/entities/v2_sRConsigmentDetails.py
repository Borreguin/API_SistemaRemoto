from mongoengine import EmbeddedDocument, StringField, DictField


class V2SRConsignmentDetails(EmbeddedDocument):
    observations = StringField(required=False, default=None)
    short_description = StringField(required=False, default=None)
    consignment_type = StringField(required=False, default=None)
    element = DictField(required=False, default=None)

    def to_dict(self):
        return dict(observations=self.observations, short_description=self.short_description,
                    consignment_type=self.consignment_type, element=self.element)