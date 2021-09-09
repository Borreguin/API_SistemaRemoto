from flask_app.dto.mongo_engine_handler.SRNodeReport.sRNodeReportBase import SRNodeDetailsBase, dt

from mongoengine import *


class SRNodeDetailsTemporal(SRNodeDetailsBase):
    # Esta configuración permite crear documentos JSON con expiración de tiempo
    created = DateTimeField(default=dt.datetime.utcnow())
    meta = {"collection": "TEMPORAL|Nodos", 'indexes': [{
        'cls': False,
        'fields': ['created'],
        'expireAfterSeconds': 31104000
    }]}
# tiempo de vida 12 meses 60*60*24*12*30 = 31104000
#'expireAfterSeconds': 60 * 60 * 24 * 6 * 30