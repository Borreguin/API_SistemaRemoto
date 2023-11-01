""" coding: utf-8
Created by rsanchez on 03/05/2018
Este proyecto ha sido desarrollado en la Gerencia de Operaciones de CENACE
Mateo633
"""

from flask_app.my_lib.PI_connection import simulation
from flask_app.my_lib.PI_connection.PIServer.PIPointBase import PIPointBase
from flask_app.my_lib.PI_connection.PIServer.PIPointSimulation import PIPointSimulation
from flask_app.my_lib.PI_connection.PIServer.PIServerBase import PIServerBase
from flask_app.my_lib.PI_connection.PIServer.PIServerSimulation import PIServerSimulation


def create_pi_server(name=None) -> PIServerBase:
    if simulation:
        return PIServerSimulation(name)
    else:
        from flask_app.my_lib.PI_connection.PIServer.PIServerWindows import PIServerWindows
        return PIServerWindows(name)


def create_pi_point(server: PIServerBase, tag_name: str) -> PIPointBase:
    if simulation:
        return PIPointSimulation(server, tag_name)
    else:
        from flask_app.my_lib.PI_connection.PIServer.PIPointWindows import PIPointWindows
        return PIPointWindows(server, tag_name)
