from dto.mongo_engine_handler.sRFinalReport import SRFinalReport
import datetime as dt

class SRFinalReportTemporal(SRFinalReport):
    # Esta configuración permite crear documentos JSON con expiración de tiempo
    meta = {"collection": "TEMPORAL|FinalReports", 'indexes': [{
        'fields': ['created'],
        # tiempo de vida 6 meses 60*60*24*6*30
        'expireAfterSeconds': 60*60*24*6*30
    }]}


