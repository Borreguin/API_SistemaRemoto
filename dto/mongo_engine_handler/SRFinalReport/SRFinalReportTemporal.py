from dto.mongo_engine_handler.SRFinalReport.sRFinalReportBase import SRFinalReportBase
import datetime as dt
from mongoengine import *


class SRFinalReportTemporal(SRFinalReportBase):
    # Esta configuración permite crear documentos JSON con expiración de tiempo
    created = DateTimeField(default=dt.datetime.now())
    meta = {"collection": "TEMPORAL|FinalReports", 'indexes': [{
        'fields': ['created'],
        'expireAfterSeconds': 60
    }]}

# tiempo de vida 6 meses 60*60*24*6*30
