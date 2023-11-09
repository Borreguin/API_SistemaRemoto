from mongoengine import connect
import warnings
from app.core.config import settings

MONGO_TESTER_DB = "DB_DISP_EMS_TEST"


def connect_to_test_db() -> bool:
    try:
        # warnings.filterwarnings("ignore", category=DeprecationWarning)
        connect(db=MONGO_TESTER_DB, host=settings.MONGO_SERVER_IP, port=int(settings.MONGO_PORT),
                alias='tester', uuidRepresentation='standard')
        return True
    except Exception as e:
        print(f"Not able to connect due to: {e}")
        return False


def connectTestDB(func):
    def wrapper(*args, **kwargs):
        print(">>>>>	Connecting to test database... ")
        connect_to_test_db()
        func(*args, **kwargs)

    return wrapper
