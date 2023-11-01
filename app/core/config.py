"""
Global settings for the application
"""
from dotenv import load_dotenv
import os
from app import project_path


def load_environmental_variables():
    environment = os.getenv('ENV', 'dev')
    if environment == 'prod':
        env_path = os.path.join(project_path, 'app', 'core', 'env', 'prod.env')
    else:
        env_path = os.path.join(project_path, 'app', 'core', 'env', 'dev.env')
    load_dotenv(dotenv_path=env_path)


class Settings:
    load_environmental_variables()
    PROJECT_NAME: str = os.getenv("PROJECT_NAME", "Not defined")
    PROJECT_VERSION: str = os.getenv("PROJECT_VERSION", "0.0.0")
    HOSTNAME: str = os.getenv("HOSTNAME", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", 5000))
    API_PREFIX: str = os.getenv("API_PREFIX", "/dev")
    MONGO_SERVER_IP: str = os.getenv("MONGO_SERVER_IP", 'localhost')
    MONGO_PORT: int = os.getenv("MONGO_PORT", 27017)
    MONGO_DB = os.getenv("MONGO_DB", 'DB_DISP_EMS')
    SQLALCHEMY_DATABASE_URL: str = os.getenv("SQLALCHEMY_DATABASE_URL", "sqlite:///./app.db")

    # Dates settings:
    SUPPORTED_FORMAT_DATES = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S.%f"]
    DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    # PIServer configurations:
    PISERVERS = ["10.1.10.108", "10.1.10.109"]


settings = Settings()
