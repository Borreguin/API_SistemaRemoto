import flask_app.settings.LogDefaultConfig
from flask_app.settings import initial_settings as init

log_master = flask_app.settings.LogDefaultConfig.LogDefaultConfig("eng_sRmaster.log").logger
log_node = flask_app.settings.LogDefaultConfig.LogDefaultConfig("eng_sRnode.log").logger