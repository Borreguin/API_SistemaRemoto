from app.common.PI_connection import pi_label
from app.common.PI_connection.PIServer.PIPointBase import PIPointBase


class PIPointSimulation(PIPointBase):

    def __init__(self, server, tag_name):
        super().__init__(server, tag_name)
        print(f"{pi_label} Simulate PIPointSimulation server: {server}, tag_name: {tag_name}")

