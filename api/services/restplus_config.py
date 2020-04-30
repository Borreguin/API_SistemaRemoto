"""
    This file defines all the API's configuration - API Home Page
    Archivo que define la configuración de la API en General - página inicial de la API
    Adds:
        Logger to save all the problems in the API
        Error handler in case is needed
"""

import traceback
from flask import request
import datetime as dt
from flask_restplus import Api
from settings.initial_settings import FLASK_DEBUG
from sqlalchemy.orm.exc import NoResultFound
from settings.initial_settings import LogDefaultConfig

api_log = LogDefaultConfig("api_services.log").logger

api = Api(version='0.1', title='API - Cálculo de disponibilidad EMS',
          contact="Roberto Sánchez A",
          contact_email="rg.sanchez.a@gmail.com",
          contact_url="https://github.com/Borreguin",
          description='Esta API permite calcular/consultar todo lo referente a la disponibilidad del EMS',
          ordered=True)


@api.errorhandler(Exception)
def default_error_handler(e):
    global api_log
    ts = dt.datetime.now().strftime('[%Y-%b-%d %H:%M:%S.%f]')
    msg = f"{ts} {request.remote_addr} {request.method} {request.scheme}" \
          f"{request.full_path}"
    api_log.error(msg)
    api_log.error(traceback.format_exc())
    if hasattr(e, 'data'):
        return dict(success=False, errors=str(e.data["errors"])), 400
    return dict(success=False, errors=str(e)), 500

    # if not FLASK_DEBUG:
    #    return {'message': message}, 500


@api.errorhandler(NoResultFound)
def database_not_found_error_handler(e):
    api_log.warning(traceback.format_exc())
    return {'message': 'No se obtuvo resultado'}, 404
