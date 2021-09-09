from flask_app.dto.mongo_engine_handler.SRNodeReport.sRNodeReportBase import SRNodeDetailsBase


class SRNodeDetailsPermanente(SRNodeDetailsBase):
    meta = {"collection": "REPORT|Nodos"}
