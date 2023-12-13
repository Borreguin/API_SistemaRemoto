import datetime as dt
from mongoengine import *

from app.db.v2.v2SRNodeReport.V2SRNodeDetailsBase import V2SRNodeDetailsBase


class V2SRNodeDetailsTemporal(V2SRNodeDetailsBase):
    # Esta configuración permite crear documentos JSON con expiración de tiempo
    created = DateTimeField(default=dt.datetime.utcnow())
    meta = {"collection": "TEMPORAL|v2Nodos", 'indexes': [{
        'cls': False,
        'fields': ['created'],
        'expireAfterSeconds': 31104000
    }]}
# tiempo de vida 12 meses 60*60*24*12*30 = 31104000
#'expireAfterSeconds': 60 * 60 * 24 * 6 * 30