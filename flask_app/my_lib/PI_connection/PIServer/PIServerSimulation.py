from flask_app.my_lib.PI_connection import pi_label
from flask_app.my_lib.PI_connection.PIServer.PIServerBase import PIServerBase


class PIServerSimulation(PIServerBase):
    def __init__(self, name=None):
        print(f"{pi_label} Simulate server name {name}")
