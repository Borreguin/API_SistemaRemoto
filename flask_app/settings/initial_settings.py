# Created by Roberto Sanchez at 3/29/2019
# -*- coding: utf-8 -*-
""" Set the initial settings of this application"""
"""
CAMBIOS EN ESTA VERSION: 
1 INCLUIR CARPETA PARA COLOCAR ARCHIVOS ADJUNTOS PARA CONSIGNACIONES

"""
import os
from .config import config as raw_config

""""
    Created by Roberto Sánchez A
    
    Any copy of this code should be notified at rg.sanchez.a@gmail.com; you can redistribute it
    and/or modify it under the terms of the MIT License.

    If you need more information. Please contact the email above: rg.sanchez.a@gmail.com
    "My work is well done to honor God at any time" R Sanchez A.
    Mateo 6:33
"""

""" script path"""
script_path = os.path.dirname(os.path.abspath(__file__))
flask_path = os.path.dirname(script_path)
project_path = os.path.dirname(flask_path)
motor_path = os.path.join(flask_path, "motor")
print("Loading configurations from: " + script_path)
""" initial configuration """
config = raw_config

""" Defining whether is the production environment or not """
production_path = os.path.join(project_path, "Production_server.txt")
print(production_path, os.path.exists(production_path))
if os.path.exists(production_path):
    PRODUCTION_ENV = True
    config["DEBUG"] = False
else:
    PRODUCTION_ENV = False

TESTING_ENV = os.getenv('testing_env', None) == 'True'

if PRODUCTION_ENV:
    dev = dict()
else:
    from flask_app.settings.dev import dev as developer_dict

    dev = developer_dict
    if TESTING_ENV:
        dev["MONGOCLIENT_SETTINGS"]["db"] = "DB_DISP_EMS"

"""" FLASK CONFIGURATION """
FLASK_SERVER_NAME = config["FLASK_SERVER_NAME"]
SECRET_KEY = os.environ['SECRET_KEY'] if not PRODUCTION_ENV else "ChAng3-Th1$-$6creTK6y"
DEBUG = config["DEBUG"]

""" Log file settings: """
sR_node_name = config["ROTATING_FILE_HANDLER"]["filename"]
log_path = os.path.join(project_path, "logs")
log_file_name = os.path.join(project_path, sR_node_name)
config["ROTATING_FILE_HANDLER"]["filename"] = log_file_name
ROTATING_FILE_HANDLER = config["ROTATING_FILE_HANDLER"]
ROTATING_FILE_HANDLER_LOG_LEVEL = config["ROTATING_FILE_HANDLER_LOG_LEVEL"]

""" Settings for Mongo Client"""
MONGOCLIENT_SETTINGS = config["MONGOCLIENT_SETTINGS"]
MONGO_LOG_LEVEL = config["MONGO_LOG_LEVEL"]["value"]
MONGO_LOG_LEVEL_OPTIONS = config["MONGO_LOG_LEVEL"]["options"]

# if DEBUG:
#    MONGOCLIENT_SETTINGS.update(dict(db="DB_DISP_EMS_TEST"))
# print('Configuraciones de MongoDB:' + str(MONGOCLIENT_SETTINGS))

""" Configuration of Mongo Engine """
"""
Connections in MongoEngine are registered globally and are identified with aliases
Therefore no need to initialize other connections. 
"""

""" SUPPORTED DATES """
SUPPORTED_FORMAT_DATES = config["SUPPORTED_FORMAT_DATES"]
DEFAULT_DATE_FORMAT = config["DEFAULT_DATE_FORMAT"]

""" SWAGGER CONFIGURATION """
RESTPLUS_SWAGGER_UI_DOC_EXPANSION = config["RESTPLUS_SWAGGER_UI_DOC_EXPANSION"]
RESTPLUS_VALIDATE = config["RESTPLUS_VALIDATE"]
RESTPLUS_MASK_SWAGGER = config["RESTPLUS_MASK_SWAGGER"]
RESTPLUS_ERROR_404_HELP = config["RESTPLUS_ERROR_404_HELP"]
API_PREFIX = config["API_PREFIX"]
PORT = config["PORT"] if PRODUCTION_ENV else config["DEBUG_PORT"]
DEBUG_PORT = config["DEBUG_PORT"]
VERSION = config["version"]

""" PIServer Config """
PISERVERS = config["PISERVERS"]

"""" EXCEL REPO CONFIGURATION """
LOGS_REPO = config["LOGS_REPO"]
EXCEL_REPO = config["EXCEL_REPO"]
SREMOTO_REPO = config["SREMOTO_EXCEL_REPO"]
SCENTRAL_REPO = config["SCENTRAL_EXCEL_REPO"]
OUTPUT_MOTOR_REPO = config["OUTPUT_MOTOR"]
CONS_REPO = config["CONSIGNACIONES"]
TEMPLATES_REPO = config["TEMPLATES_REPO"]
IMAGES_REPO = config["IMAGES_REPO"]
REPORTS_REPO = config["REPORTS_REPO"]
TEMP_REPO = "temp"

""" LISTA DE REPOSITORIOS """
REPOS = [LOGS_REPO, EXCEL_REPO, SREMOTO_REPO, SCENTRAL_REPO, OUTPUT_MOTOR_REPO, CONS_REPO, TEMPLATES_REPO, IMAGES_REPO,
         REPORTS_REPO, TEMP_REPO]
FINAL_REPO = list()
for repo in REPOS:
    this_repo = os.path.join(project_path, repo)
    if not os.path.exists(this_repo):
        os.makedirs(this_repo)
    FINAL_REPO.append(this_repo)

# getting the definitive path for each one in same order:
LOGS_REPO, EXCEL_REPO, SREMOTO_REPO, SCENTRAL_REPO, OUTPUT_MOTOR_REPO, CONS_REPO, TEMPLATES_REPO, \
IMAGES_REPO, REPORTS_REPO, TEMP_REPO = FINAL_REPO
