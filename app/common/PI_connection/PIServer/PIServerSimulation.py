from PIServer.PIServerBase import PIServerBase

pi_label = "PI-connect ->"

class PIServerSimulation(PIServerBase):
    def __init__(self, name=None):
        print(f"{pi_label} Simulate server name {name}")
