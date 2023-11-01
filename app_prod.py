import os
import uvicorn

from app.core.config import Settings


def run_production_api():
    os.environ["ENV"] = 'prod'
    host = Settings.HOSTNAME
    port = Settings.API_PORT
    print(f">>>>> \tUI API deployed over: http://{host}:{port}/docs")
    uvicorn.run("app.main:api", host=host, port=port)


if __name__ == "__main__":
    run_production_api()
