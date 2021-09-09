# -*- coding: utf-8 -*-
import os
"""
CAMBIOS EN ESTA VERSION 

"""
config = dict()

config["name"] = "settings"
config["version"] = "0.3"

config["FLASK_SERVER_NAME"] = "localhost:7077"
#Configuración de Debbugging
config["DEBUG"] = True
#Configuración de UI de SWAGGER
config["RESTPLUS_SWAGGER_UI_DOC_EXPANSION"] = "list"
config["RESTPLUS_VALIDATE"] = True
config["RESTPLUS_MASK_SWAGGER"] = False
config["RESTPLUS_ERROR_404_HELP"] = False
config["API_PREFIX"] = '/api-rmt'
config["API_PORT"] = 7820
config["DEBUG_PORT"] = 5000

config["ROTATING_FILE_HANDLER_HELP"] = "https://docs.python.org/3.6/library/logging.handlers.html#logging.handlers.RotatingFileHandler.__init__",
config["ROTATING_FILE_HANDLER"] = {"filename": "app_flask.log", "maxBytes": 5000000, "backupCount": 5, "mode": "a"}
config["ROTATING_FILE_HANDLER_LOG_LEVEL"] = {"value": "info", "options": ["error", "warning", "info", "debug", "off"]}

# MONGODB CONFIGURATION
config["MONGOCLIENT_SETTINGS"] = {"host": "localhost", "port": 2717, "db": "DB_DISP_EMS"}
config["MONGO_LOG_LEVEL"] = {"value": "OFF", "options": ["ON", "OFF"]}

# Excel repository:

config["DB_REPO"] = "_db"
config["LOGS_REPO"] = "logs"
config["EXCEL_REPO"] = os.path.join(config["DB_REPO"], "excel_files")
config["SREMOTO_EXCEL_REPO"] = os.path.join(config["EXCEL_REPO"], "s_remoto_excel")
config["SCENTRAL_EXCEL_REPO"] = os.path.join(config["EXCEL_REPO"], "s_central_excel")
config["OUTPUT_MOTOR"] = os.path.join(config["LOGS_REPO"], "output")
config["CONSIGNACIONES"] = os.path.join(config["DB_REPO"], "consignaciones")
config["TEMPLATES_REPO"] = os.path.join("flask_app", "templates")
config["REPORTS_REPO"] = "reports"
config["IMAGES_REPO"] = os.path.join(config["REPORTS_REPO"], "images")


config["SUPPORTED_FORMAT_DATES"] = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S.%f"]
config["DEFAULT_DATE_FORMAT"] = "%Y-%m-%d %H:%M:%S"

# PIServer configurations:
config["PISERVERS"] = ["10.1.10.108", "10.1.10.109"]