from app.common.default_logger import configure_logger
print(f">>>>> \tDefault loggers has been created")
app_log = configure_logger("fastApi_activity.log")
error_log = configure_logger("errors.log")
report_log = configure_logger("v2_report.log")
report_node_log = configure_logger("v2_node.log")
report_node_detail_log = configure_logger("v2_node_detail.log")
