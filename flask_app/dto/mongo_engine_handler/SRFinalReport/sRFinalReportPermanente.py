from flask_app.dto.mongo_engine_handler.SRFinalReport.sRFinalReportBase import SRFinalReportBase


class SRFinalReportPermanente(SRFinalReportBase):
    meta = {"collection": "REPORT|FinalReports"}
