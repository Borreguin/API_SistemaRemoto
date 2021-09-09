from flask_app.dto.mongo_engine_handler.SRNodeReport.sRNodeReportBase import SRNodeDetailsBase, dt

from mongoengine import *


class SRNodeDetailsTemporal(SRNodeDetailsBase):
    # Esta configuración permite crear documentos JSON con expiración de tiempo
    created = DateTimeField(default=dt.datetime.utcnow())
    meta = {"collection": "TEMPORAL|Nodos", 'indexes': [{
        'cls': False,
        'fields': ['created'],
        'expireAfterSeconds': 15552000
    }]}
# tiempo de vida 6 meses 60*60*24*6*30 = 15552000
#'expireAfterSeconds': 60 * 60 * 24 * 6 * 30