from fastapi import FastAPI
from starlette.requests import Request

from app.common import app_log


def log_after_request(app: FastAPI):
    # This logs any activity of the app
    @app.middleware("http")
    async def log_activity_for_this_call(request: Request, call_next):
        response = await call_next(request)
        app_log.info(f"{request.client.host}: {request.method} {request.url} [{response.status_code}]")
        return response
