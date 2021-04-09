import traceback

from mongoengine import *
import datetime as dt


class TemporalProcessingStateReport(Document):
    id_report = StringField(required=True, unique=True, default=None)
    percentage = FloatField(required=True, min_value=0, max_value=100, default=0)
    created = DateTimeField(default=dt.datetime.now())
    modified = DateTimeField(default=dt.datetime.now())
    processing = BooleanField(default=True)
    fail = BooleanField(default=False)
    finish = BooleanField(default=False)
    info = DictField()
    msg = StringField()
    # Esta configuración permite crear documentos JSON con expiración de tiempo
    meta = {"collection": "STATUS|temporal", 'indexes': [{
        'fields': ['created'],
        'expireAfterSeconds': 360000
    }]}

    def     __init__(self, *args, **values):
        super().__init__(*args, **values)
        if self.created is None:
            self.created = dt.datetime.now()

    def to_dict(self):
        return dict(id_report=self.id_report, percentage=self.percentage,
                    created=self.created, modified=self.created,
                    processing=self.processing, fail=self.fail,
                    finish=self.finish, info=self.info, msg=self.msg)

    def to_summary(self):
        return dict(id_report=self.id_report, percentage=self.percentage,
                    created=str(self.created), modified=str(self.created),
                    processing=self.processing, fail=self.fail,
                    finish=self.finish, info=self.info, msg=self.msg)

    def finished(self):
        self.finish = True
        self.processing = False
        self.fail = False
        self.percentage = 100
        self.modified = dt.datetime.now()

    def failed(self):
        self.finish = True
        self.processing = False
        self.fail = True
        self.percentage = 100
        self.modified = dt.datetime.now()

    def update_now(self):
        tmp = TemporalProcessingStateReport.objects(id_report=self.id_report).first()
        try:
            if tmp is None:
                self.save()
            else:
                tmp.update(**self.to_dict())
        except Exception as e:
            print(e)
            print(traceback.format_exc())
