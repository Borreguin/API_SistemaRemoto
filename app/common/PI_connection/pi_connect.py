""" coding: utf-8
Created by rsanchez on 03/05/2018
Este proyecto ha sido desarrollado en la Gerencia de Operaciones de CENACE
Mateo633
"""
import sys

from PIServer.PIPointBase import PIPointBase
from PIServer.PIPointSimulation import PIPointSimulation
from PIServer.PIServerBase import PIServerBase
from PIServer.PIServerSimulation import PIServerSimulation

simulation = True if sys.platform == 'win32' else True

def create_pi_server(name=None) -> PIServerBase:
    if simulation:
        return PIServerSimulation(name)
    else:
        from PIServer.PIServerWindows import PIServerWindows
        return PIServerWindows(name)


def create_pi_point(server: PIServerBase, tag_name: str) -> PIPointBase:
    if simulation:
        return PIPointSimulation(server, tag_name)
    else:
        from PIServer.PIPointWindows import PIPointWindows
        return PIPointWindows(server, tag_name)
