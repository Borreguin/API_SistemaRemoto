import os
from app_dev import run_development_api
from app_prod import run_production_api

if __name__ == "__main__":
    environment = os.getenv("ENV", 'dev')
    if environment == 'prod':
        run_production_api()
    else:
        run_development_api()
