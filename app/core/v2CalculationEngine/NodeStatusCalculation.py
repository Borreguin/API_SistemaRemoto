from enum import Enum


class NodeStatusCalculation(str, Enum):
    OK = "Nodo procesado correctamente"
    CONNECTED = "Connected"
    ERROR = "Error no determinado"
    NOT_FOUND = "No se encontro el nodo"
    NOT_RECOGNIZED = "Objeto nodo no reconocido"
    NO_PI_CONNECTION = "No es posible la conexión con el servidor PI"
    NO_ENTITIES = "No se ha obtenido las entidades en el nodo"
    NO_CALCULATED_ENTITIES = "No se pudo calcular las entidades, revise archivo Log"
    NO_SAVE = "No se ha podido guardar el reporte del nodo"
    REPORT_EXIST = "No ha sido calculado, el reporte ya existe en DB"
    SAVED = "Reporte guardado en base de datos"
    OVERWRITTEN = "Reporte sobrescrito en base de datos"
    NO_CONSIGNATION_CONT = "Las UTR no contienen contenedor de consignaciones"
    NO_DATA_BASE_CONNECTION = "No se pudo conectar a la base de datos"

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value