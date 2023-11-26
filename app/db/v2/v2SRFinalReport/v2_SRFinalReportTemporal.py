import datetime as dt
from mongoengine import *

from app.db.v2.v2SRFinalReport.v2_sRFinalReportBase import V2SRFinalReportBase


class V2SRFinalReportTemporal(V2SRFinalReportBase):
    # Esta configuración permite crear documentos JSON con expiración de tiempo
    created = DateTimeField(default=dt.datetime.utcnow())
    meta = {"collection": "TEMPORAL|v2FinalReports", 'indexes': [{
        'cls': False,
        'fields': ['created'],
        'expireAfterSeconds': 31104000
    }]}

# tiempo de vida 12 meses 60*60*24*12*30 = 31104000
