from PIServer.PIPointBase import PIPointBase
pi_label = "PI-connect ->"

class PIPointSimulation(PIPointBase):

    def __init__(self, server, tag_name):
        super().__init__(server, tag_name)
        print(f"{pi_label} Simulate PIPointSimulation server: {server}, tag_name: {tag_name}")

