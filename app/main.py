from fastapi import FastAPI
from mongoengine import connect, disconnect

from app.common import app_log
# import general settings
from app.core.log_after_request import log_after_request
from app.core.config import settings
from app.core.exception_handler import define_handler_exception

# import endpoints
from app.endpoints import Files
from app.endpoints import AdminSRemoto
from app.endpoints.AdminConsignacion import AdminConsignacion
from app.endpoints.AdminReport import AdminReport
from app.endpoints.DispSRemoto import DispSRemoto
from app.endpoints.SRemoto import SRemoto


def include_routes(app):
    # To include EndPoints:
    app.include_router(AdminSRemoto.router)
    app.include_router(AdminReport.router)
    app.include_router(AdminConsignacion.router)
    app.include_router(DispSRemoto.router)
    app.include_router(Files.router)
    app.include_router(SRemoto.router)


def db_connection(n_try=1):
    if n_try >= 3:
        app_log.warn(f"MongoDB: Not able to connect with database")
        return False

    try:
        connect(db=settings.MONGO_DB, host=settings.MONGO_SERVER_IP, port=int(settings.MONGO_PORT), alias='default')
        app_log.info('MongoDB: Connection accepted')
        return True
    except Exception as e:
        app_log.warn(f"MongoDB: {e}, try reconnection.")
        disconnect(alias='default')
        return db_connection(n_try + 1)


def define_loggers(app):
    # adds general handler exception if something was not controlled
    define_handler_exception(app)
    # adds logger for requests
    log_after_request(app)


def create_application() -> FastAPI:
    contact = dict(name="Roberto Sánchez A", email="rg.sanchez.a@gmail.com",
                   url="https://github.com/Borreguin/API_SistemaRemoto")
    description = "Esta API permite calcular/consultar todo lo referente a la disponibilidad de Sistema Remoto"
    app = FastAPI(title=settings.PROJECT_NAME, version=settings.PROJECT_VERSION, contact=contact,
                  description=description)
    define_loggers(app)
    include_routes(app)
    db_connection()
    return app


api = create_application()
