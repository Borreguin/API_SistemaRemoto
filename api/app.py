""""
      Created by Roberto Sánchez A.
      API de disponibilidad: Desplega las funciones necesarias para el cáclulo de la disponibilidad
      Servicios:
        - Sistema Remoto: Implementa lo referente a sistema remoto
        - Sistema Central: Implementa lo referente a sistema Central
"""
from flask import Flask
from flask import send_from_directory
import os, sys
from flask import Blueprint
from flask_mongoengine import MongoEngine
# añadiendo a sys el path del proyecto:
# permitiendo el uso de librerías propias:
api_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(api_path)
sys.path.append(api_path)
sys.path.append(project_path)

from settings import initial_settings as init
# importando la configuración general de la API
from api.services.restplus_config import api as api_p
import datetime as dt
from flask import request
from waitress import serve

# namespaces: Todos los servicios de esta API
from api.services.sRemoto.endpoints.api_sR_admin import ns as namespace_sR_admin
from api.services.sRemoto.endpoints.api_sR_cal_disponibilidad import ns as namespace_sR_cal_disponibilidad
from api.services.Files.api_files import ns as namespace_files
from api.services.Diagrams.endpoints.api_diagrams import ns as namespace_diagrams

""" global variables """
app = Flask(__name__)                                                   # Flask application
log = init.LogDefaultConfig("app_flask.log").logger                                    # Logger
blueprint = Blueprint('api', __name__, url_prefix='/api')               # Name Space for API


@blueprint.route("/test")
def b_test():
    """
        To know whether the Blueprint is working or not Ex: http://127.0.0.1:5000/api/test
    """
    return "This is a test. Blueprint is working correctly."


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')


def configure_api_app():
    """
    Configuración general de la aplicación API - SWAGGER
    :return:
    """
    global app
    app.config['SWAGGER_UI_DOC_EXPANSION'] = init.RESTPLUS_SWAGGER_UI_DOC_EXPANSION
    app.config['RESTPLUS_VALIDATE'] = init.RESTPLUS_VALIDATE
    app.config['RESTPLUS_MASK_SWAGGER'] = init.RESTPLUS_MASK_SWAGGER
    app.config['ERROR_404_HELP'] = init.RESTPLUS_ERROR_404_HELP


def configure_home_api_swagger():
    """
    Configuración de la API. Añadiendo los servicios a la página inicial
    Aquí añadir todos los servicios que se requieran para la API:
    """
    # añadiendo la ruta blueprint (/api)
    global blueprint
    api_p.init_app(blueprint)

    # añadiendo los servicios de cálculo:
    api_p.add_namespace(namespace_sR_admin)
    api_p.add_namespace(namespace_sR_cal_disponibilidad)
    api_p.add_namespace(namespace_files)
    api_p.add_namespace(namespace_diagrams)

    # registrando las rutas:
    app.register_blueprint(blueprint)


def configure_mongo_engine():
    global app
    app.config['MONGODB_SETTINGS'] = init.MONGOCLIENT_SETTINGS
    db = MongoEngine(app)


@app.route("/")
def main_page():
    """ Adding initial page """
    return "Hello from FastCGI via IIS! Testing an empty web Page 20"


@app.after_request
def after_request(response):
    """ Logging after every request. """
    # This avoids the duplication of registry in the log,
    # since that 500 is already logged via @logger_api
    ts = dt.datetime.now().strftime('[%Y-%b-%d %H:%M:%S.%f]')
    msg = f"{ts} {request.remote_addr} {request.method} {request.scheme}" \
          f"{request.full_path} {response.status}"
    if 200 >= response.status_code < 400:
        log.info(msg)
    elif 400 >= response.status_code < 500:
        log.warning(msg)
    elif response.status_code >= 500:
        log.error(msg)
    return response


def main():
    # aplicando la configuración general:
    configure_api_app()
    # añadiendo los servicios necesarios:
    configure_home_api_swagger()
    # iniciando la API
    log.info('>>>>> Starting development server at http://{}/services/ <<<<<'.format(app.config['SERVER_NAME']))
    # iniciando base de datos Mongo
    configure_mongo_engine()
    if init.MONGO_LOG_LEVEL == "ON":
        print("WARNING!! El log de la base de datos MongoDB está activado. "
              "Esto puede llenar de manera rápida el espacio en disco")

    # serve the application
    if init.FLASK_DEBUG:
        app.run(debug=init.FLASK_DEBUG)
    else:
        serve(app, host='0.0.0.0', port=5000)


if __name__ == "__main__":
    main()
