""" General imports """
import os
import sys

# añadiendo a sys el path del proyecto:
# permitiendo el uso de librerías propias:
import flask_app.settings.LogDefaultConfig

api_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(api_path)
sys.path.append(api_path)
sys.path.append(project_path)

""" global variables """
from flask_app.settings import initial_settings as init

log = flask_app.settings.LogDefaultConfig.LogDefaultConfig("app_flask.log").logger
from flask_app.api.app_config import create_app

app = create_app()
