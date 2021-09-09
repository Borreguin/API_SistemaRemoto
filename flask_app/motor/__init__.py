import flask_app.settings.LogDeafultConfig
from flask_app.settings import initial_settings as init

log_master = flask_app.settings.LogDeafultConfig.LogDefaultConfig("eng_sRmaster.log").logger
log_node = flask_app.settings.LogDeafultConfig.LogDefaultConfig("eng_sRnode.log").logger