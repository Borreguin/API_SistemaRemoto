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
import hashlib

from mongoengine import *
import datetime as dt


class Consignment(EmbeddedDocument):
    no_consignacion = StringField(required=True)
    fecha_inicio = DateTimeField(required=True, default=dt.datetime.now())
    fecha_final = DateTimeField(required=True, default=dt.datetime.now())
    t_minutos = IntField(required=True)
    id_consignacion = StringField(default=None, required=True)
    detalle = DictField()

    def __init__(self, *args, **values):
        super().__init__(*args, **values)
        if self.id_consignacion is None:
            # print(self.no_consignacion)
            if self.no_consignacion is None:
                return
            id = self.no_consignacion + str(self.fecha_inicio) + str(self.fecha_final)
            self.id_consignacion = hashlib.md5(id.encode()).hexdigest()
        self.calculate()

    def calculate(self):
        if self.fecha_inicio is None or self.fecha_final is None:
            return
        if isinstance(self.fecha_inicio, str):
            self.fecha_inicio = dt.datetime.strptime(self.fecha_inicio, "%Y-%m-%d %H:%M:%S")
        if isinstance(self.fecha_final, str):
            self.fecha_final = dt.datetime.strptime(self.fecha_final, "%Y-%m-%d %H:%M:%S")

        if self.fecha_inicio >= self.fecha_final:
            raise ValueError("La fecha de inicio no puede ser mayor o igual a la fecha de fin")
        t = self.fecha_final - self.fecha_inicio
        self.t_minutos = t.days * (60 * 24) + t.seconds // 60 + t.seconds % 60

    def __str__(self):
        return f"({self.no_consignacion}: min={self.t_minutos}) [{self.fecha_inicio.strftime('%d-%m-%Y %H:%M')}, " \
               f"{self.fecha_final.strftime('%d-%m-%Y %H:%M')}]"

    def to_dict(self):
        return dict(no_consignacion=self.no_consignacion,
                    fecha_inicio=str(self.fecha_inicio), fecha_final=str(self.fecha_final),
                    id_consignacion=self.id_consignacion,
                    detalle=self.detalle)


class Consignments(Document):
    id_elemento = StringField(required=True, unique=True)
    elemento = DictField(required=False)
    consignacion_reciente = EmbeddedDocumentField(Consignment)
    consignaciones = ListField(EmbeddedDocumentField(Consignment))
    meta = {"collection": "INFO|Consignaciones"}

    def get_last_consignment(self):
        t, ixr = dt.datetime(1900, 1, 1), -1
        for ix, c in enumerate(self.consignaciones):
            # check last date:
            if c.fecha_final > t:
                t, ixr = c.fecha_final, ix
        if ixr != -1:
            self.consignacion_reciente = self.consignaciones[ixr]

    def insert_consignments(self, consignacion: Consignment):
        # si es primera consignacion a insertar
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
            return False, f"No existe la consignación [{no_consignacion}] en elemento [{self.id_elemento}]"
        self.consignaciones = new_consignaciones
        return True, f"Consignación [{no_consignacion}] ha sido eliminada"

    def consignments_in_time_range(self, ini_date: dt.datetime, end_time: dt.datetime):
        return [c for c in self.consignaciones if
                ini_date <= c.fecha_inicio < end_time or ini_date < c.fecha_final <= end_time]

    def search_consignment_by_id(self, id_to_search):
        for consignment in self.consignaciones:
            if consignment.id_consignacion == id_to_search:
                return True, consignment
        return False, None

    def remove_consignment_by_id(self, id_to_delete):
        for ix, consignment in enumerate(self.consignaciones):
            if consignment.id_consignacion == id_to_delete:
                new_consignments = self.consignaciones[0:ix] + self.consignaciones[(ix+1):]
                self.consignaciones = new_consignments
                return True, f"Consignación {consignment.no_consignacion} eliminada existosamente"
        return False, f"No se encontró la consignación: {id_to_delete}"

    def edit_consignment(self, id_to_edit, consignment: Consignment):
        revert = self.consignaciones
        success, msg = self.remove_consignment_by_id(id_to_edit)
        if not success:
            return False, msg
        success, msg = self.insert_consignments(consignment)
        if success:
            return True, f"La consignación {consignment.no_consignacion} ha sido editada correctamente"
        else:
            self.consignaciones = revert
            return False, msg

    def __str__(self):
        return f"{self.id_elemento}: ({self.consignacion_reciente}) [{len(self.consignaciones)}]"
