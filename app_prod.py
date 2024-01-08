import os
import uvicorn


def run_production_api():
    os.environ["ENV"] = 'prod'
    from app.core.config import Settings
    host = Settings.HOSTNAME
    port = Settings.API_PORT
    print(f">>>>> \tUI API deployed over: http://{host}:{port}/docs")
    uvicorn.run("app.main:api", host=host, port=port)


if __name__ == "__main__":
    run_production_api()
