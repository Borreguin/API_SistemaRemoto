from fastapi import FastAPI
from starlette import status
from starlette.requests import Request
from starlette.responses import JSONResponse
import traceback

from app.common import error_log


def define_handler_exception(app: FastAPI):
    # This dispatch all general Exceptions
    @app.exception_handler(Exception)
    async def default_handler_exception(request: Request, exc):
        error_log.error(f"Exception: {exc}")
        error_log.error(f"{request.client} ")
        error_log.error(f"{request.method} {request.url}")
        error_log.error(f"{request.headers}")
        error_log.error(f"{traceback.format_exc(-5)}")
        return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            content=dict(error=f"{exc}"))


