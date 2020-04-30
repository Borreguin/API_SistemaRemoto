"""
Desarrollado en la Gerencia de Desarrollo Técnico
by: Roberto Sánchez Abril 2020
motto:
"Whatever you do, work at it with all your heart, as working for the Lord, not for human master"
Colossians 3:23

Consignación:
•	DOCUMENTO TIPO JSON
•	Permite indicar tiempos de consignación donde el elemento no será consgnado para el cálculo de disponibilidad

"""
from mongoengine import *
import datetime as dt


class Consignment(EmbeddedDocument):
    no_consignacion = StringField()
    fecha_inicio = DateTimeField(required=True)
    fecha_final = DateTimeField(required=True)
    t_minutos = IntField(required=True)
    detalle = StringField()

    def __init__(self, *args, **values):
        super().__init__(*args, **values)
        self.calculate()

    def calculate(self):
        if self.fecha_inicio >= self.fecha_final:
            raise ValueError("La fecha de inicio no puede ser mayor o igual a la fecha de fin")
        t = self.fecha_final - self.fecha_inicio
        self.t_minutos = t.days*(60*24) + t.seconds//60 + t.seconds % 60

    def __str__(self):
        return f"({self.no_consignacion}: min={self.t_minutos}) [{self.fecha_inicio.strftime('%d-%m-%Y %H:%M')}, " \
               f"{self.fecha_final.strftime('%d-%m-%Y %H:%M')}]"

    def to_dict(self):
        return dict(no_consignacion=self.no_consignacion,
                    fecha_inicio=self.fecha_inicio, fecha_final=self.fecha_final,
                    detalle=self.detalle)


class Consignments(Document):
    id_entidad = StringField(required=True, unique=True)
    consignacion_reciente = EmbeddedDocumentField(Consignment)
    consignaciones = ListField(EmbeddedDocumentField(Consignment))
    meta = {"collection": "INFO|Consignaciones"}

    def get_last_consignment(self):
        t, ixr = dt.datetime(1900,1,1), -1
        for ix, c in enumerate(self.consignaciones):
            # check last date:
            if c.fecha_final > t:
                t, ixr = c.fecha_final, ix
        if ixr != -1:
            self.consignacion_reciente = self.consignaciones[ixr]

    def insert_consignments(self, consignacion: Consignment):
        # si es primera consignacion insertar
        if len(self.consignaciones) == 0:
            self.consignaciones.append(consignacion)
            self.get_last_consignment()
            return True, f"Consignación insertada: {consignacion}"
        where = 0
        for ix, c in enumerate(self.consignaciones):
            # check si no existe overlapping
            incorrect_ini = c.fecha_inicio <= consignacion.fecha_inicio < c.fecha_final
            incorrect_end = c.fecha_inicio < consignacion.fecha_final <= c.fecha_final
            # check si no existe closure
            incorrect_closure = consignacion.fecha_inicio < c.fecha_inicio and consignacion.fecha_final > c.fecha_final
            # si existe overlapping or closure no se puede ingresar
            if incorrect_ini or incorrect_end or incorrect_closure:
                return False, f"{consignacion} Conflicto con la consignacion: {c}"
            # evaluar donde se puede ingresar
            correct_ini = consignacion.fecha_inicio > c.fecha_inicio
            correct_end = consignacion.fecha_final > c.fecha_inicio
            if correct_ini and correct_end:
                where = ix + 1
        if 0 <= where < len(self.consignaciones):
            self.consignaciones = self.consignaciones[0:where] + [consignacion] + self.consignaciones[where:]
        else:
            self.consignaciones.append(consignacion)
        self.get_last_consignment()
        return True, f"Consignación insertada: {consignacion}"

    def delete_consignment(self, no_consignacion):
        new_consignaciones = [c for c in self.consignaciones if c.no_consignacion != no_consignacion]
        if len(new_consignaciones) == len(self.consignaciones):
            return False, f"No existe la consignación [{no_consignacion}] en la entidad [{self.id_entidad}]"
        self.consignaciones = new_consignaciones
        return True, f"Consignación [{no_consignacion}] ha sido eliminada"

    def consignments_in_time_range(self, ini_date:dt.datetime, end_time:dt.datetime):
        return [c for c in self.consignaciones if ini_date <= c.fecha_inicio < end_time or  ini_date < c.fecha_final <= end_time]


    def __str__(self):
        return f"{self.id_entidad}: ({self.consignacion_reciente}) [{len(self.consignaciones)}]"