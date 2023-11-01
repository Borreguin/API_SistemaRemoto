from app.common.default_logger import configure_logger
print(f">>>>> \tDefault loggers has been created")
app_log = configure_logger("fastApi_activity.log")
error_log = configure_logger("errors.log")
