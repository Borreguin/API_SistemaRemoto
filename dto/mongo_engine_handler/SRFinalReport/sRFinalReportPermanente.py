from dto.mongo_engine_handler.SRFinalReport.sRFinalReportBase import SRFinalReportBase
from dto.mongo_engine_handler.sRNode import *
import hashlib

class SRFinalReportPermanente(SRFinalReportBase):
    meta = {"collection": "REPORT|FinalReports"}
