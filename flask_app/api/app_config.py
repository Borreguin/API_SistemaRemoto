# This script allows the flask configuration
# Created by Roberto Sánchez A.
# Gerencia de Desarrollo Técnico - CENACE
# Marzo 2021

# General imports:
import re
import traceback

from flask import Flask
from flask import request

# import custom configuration:
from flask_mongoengine import MongoEngine
from mongoengine import NotUniqueError
from pymongo import MongoClient

import flask_app.settings.LogDefaultConfig
from flask_app.settings import initial_settings as init

# log events:
log = flask_app.settings.LogDefaultConfig.LogDefaultConfig("app_activity.log").logger               # Activity Logger
error_log = flask_app.settings.LogDefaultConfig.LogDefaultConfig("app_errors.log").logger        # Error logger
mongo_client = init.MONGOCLIENT_SETTINGS.copy()


# API Configurations
def create_app():
    app = Flask(__name__)                   # Flask application
    app = configure_app(app)                # general swagger configuration
    db_configurations(app)                  # database configurations
    log_after_request(app)                  # configurations for log after request
    log_default_error_handler(app)          # using a default error handler in case of error
    return app


def configure_app(app):
    """
    Configuración general de la aplicación API - SWAGGER
    :return:
    """
    app.config['SWAGGER_UI_DOC_EXPANSION'] = init.RESTPLUS_SWAGGER_UI_DOC_EXPANSION
    app.config['RESTPLUS_VALIDATE'] = init.RESTPLUS_VALIDATE
    app.config['RESTPLUS_MASK_SWAGGER'] = init.RESTPLUS_MASK_SWAGGER
    app.config['ERROR_404_HELP'] = init.RESTPLUS_ERROR_404_HELP
    app.config['SECRET_KEY'] = init.SECRET_KEY
    return app


def db_configurations(app):
    app.config['MONGODB_SETTINGS'] = init.MONGOCLIENT_SETTINGS
    db = MongoEngine(app)
    if init.MONGO_LOG_LEVEL == "ON":
        print("WARNING!! El log de la base de datos MongoDB está activado. "
              "Esto puede llenar de manera rápida el espacio en disco")


def log_after_request(app):

    @app.after_request
    def after_request(response):
        """ Logging after every request. """
        # This avoids the duplication of registry in the log,
        # since that 500 is already logged via @logger_api
        # ts = dt.datetime.now().strftime('[%Y-%b-%d %H:%M:%S.%f]')
        msg = f"{request.remote_addr} {request.method} {request.scheme}" \
              f"{request.full_path} {response.status}"
        if response.status_code < 400:
            log.info(msg)
        elif response.status_code < 500:
            log.warning(msg)
        elif response.status_code >= 500:
            log.error(msg)
        return response


def log_default_error_handler(app):
    @app.errorhandler(NotUniqueError)
    def not_unique_error_handler(e):
        msg = f" {request.remote_addr} {request.method} {request.scheme}" \
              f"{request.full_path} \n{str(e)}"
        error_log.error(msg)
        success, msg, details = get_conflict_in_db(e)
        error_log.error(msg)
        return dict(success=False, msg=msg, details=details), 409

    @app.errorhandler(Exception)
    def default_error_handler(e):
        # ts = dt.datetime.now().strftime('[%Y-%b-%d %H:%M:%S.%f]')
        msg = f" {request.remote_addr} {request.method} {request.scheme}" \
              f"{request.full_path} \n{str(e)}"
        error_log.error(msg)
        if hasattr(e, 'data'):
            return dict(success=False, msg=str(e.data["errors"])), 400

        error_log.error(traceback.format_exc())
        return dict(success=False, msg=str(e)), 500


def get_conflict_in_db(e):
    try:
        # Extraendo los datos de DB y la colleccion
        r_exp = "(collection:)(.*?)(index:)"
        rsp = re.search(r_exp, str(e)).groups()
        if len(rsp) == 3:
            db, collection = rsp[1].strip().split(".")
        else:
            return False, str(e), dict()
        # Extraendo la llave valor para filtrar
        r_exp = "(key: {)(.*?)(},)"
        rsp = re.search(r_exp, str(e)).groups()
        if len(rsp) == 3:
            key, value = rsp[1].strip().split(":")
        else:
            return False, str(e), dict()
        filter_dict = {key.strip(): value.replace('"', "").strip()}
        mongo_client.pop('db', None)
        client = MongoClient(**mongo_client)
        collection_to_search = client[db][collection]
        conflict_object = collection_to_search.find_one(filter_dict)
        client.close()
        to_send = dict()
        for n, k in enumerate(conflict_object.keys()):
            if n > 4:
                break
            elif "id" not in k:
                to_send[k] = str(conflict_object[k])
        basic_info = filter_dict.copy()
        basic_info.update(to_send)
        to_send["más_detalles"] = str(conflict_object["_id"])
        to_send["conflicto"] = filter_dict
        return True, f"Elemento duplicado en conflicto con: {basic_info}", to_send
    except Exception as e:
        detalle = f"{str(e)} \n {traceback.format_exc()}"
        error_log.error(detalle)
        return False, f"No se pudo determinar el conflicto", dict()