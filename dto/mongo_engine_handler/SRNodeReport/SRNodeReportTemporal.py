from dto.mongo_engine_handler.SRNodeReport.sRNodeReportBase import SRNodeDetailsBase, dt

from mongoengine import *


class SRNodeDetailsTemporal(SRNodeDetailsBase):
    # Esta configuración permite crear documentos JSON con expiración de tiempo
    created = DateTimeField(default=dt.datetime.now())
    meta = {"collection": "TEMPORAL|Nodos", 'indexes': [{
        'fields': ['created'],
        'expireAfterSeconds': 60
    }]}
# tiempo de vida 6 meses 60*60*24*6*30
#'expireAfterSeconds': 60 * 60 * 24 * 6 * 30