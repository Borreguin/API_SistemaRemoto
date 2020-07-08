config = dict()

config["name"] = "settings"
config["version"] = "0.2"

config["FLASK_SERVER_NAME"] = "localhost:7077"
config["FLASK_DEBUG"] = True

config["RESTPLUS_SWAGGER_UI_DOC_EXPANSION"] = "list"
config["RESTPLUS_VALIDATE"] = True
config["RESTPLUS_MASK_SWAGGER"] = False
config["RESTPLUS_ERROR_404_HELP"] = False

config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///db.sqlite"
config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

config["ROTATING_FILE_HANDLER_HELP"] = "https://docs.python.org/3.6/library/logging.handlers.html#logging.handlers.RotatingFileHandler.__init__",
config["ROTATING_FILE_HANDLER"] = {"filename": "app_flask.log", "maxBytes": 5000000, "backupCount": 5, "mode": "a"}
config["ROTATING_FILE_HANDLER_LOG_LEVEL"] = {"value": "info", "options": ["error", "warning", "info", "debug", "off"]}

# MONGODB CONFIGURATION
config["MONGOCLIENT_SETTINGS"] = {"host": "localhost", "port": 2717, "db": "DB_DISP_EMS"}
config["MONGO_LOG_LEVEL"] = {"value": "OFF", "options": ["ON", "OFF"]}

# Excel repository:
config["EXCEL_REPO"] = "_db\\excel_files"
config["SREMOTO_EXCEL_REPO"] = config["EXCEL_REPO"] + "\\s_remoto_excel"
config["SCENTRAL_EXCEL_REPO"] = config["EXCEL_REPO"] + "\\s_central_excel"

config["SUPPORTED_FORMAT_DATES"] = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S.%f"]
config["DEFAULT_DATE_FORMAT"] = "%Y-%m-%d %H:%M:%S"

# PIServer configurations:
config["PISERVERS"] = ["10.1.10.108", "10.1.10.109"]