from __future__ import annotations
from app.db.v2.v2SRNodeReport.Details.v2_sRBahiaReportDetails import V2SRBahiaReportDetails


class BahiaResult:
    def __init__(self, success: bool, msg: str, bahia_report: V2SRBahiaReportDetails | None):
        self.success = success
        self.msg = msg
        self.bahia_report = bahia_report

    def get_values(self):
        return self.success, self.msg, self.bahia_report