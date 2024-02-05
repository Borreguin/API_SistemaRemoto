from __future__ import annotations
import os
from shutil import rmtree
from typing import List

from mongoengine import Document, StringField, DateTimeField, DictField, EmbeddedDocumentField, ListField

from app.db.constants import V2_SR_CONSIGNMENT_LABEL
from app.db.v2.entities.v2_sRConsignment import V2SRConsignment
import datetime as dt

from app.utils.utils import check_date


def check_consignment_conflict(consignment: V2SRConsignment, consignments: List[V2SRConsignment]):
    for c in consignments:
        # check si no existe overlapping
        overlapped_ini = c.fecha_inicio <= consignment.fecha_inicio < c.fecha_final
        overlapped_end = c.fecha_inicio < consignment.fecha_final <= c.fecha_final
        # check si no existe closure
        outside_period = consignment.fecha_inicio < c.fecha_inicio and c.fecha_final < consignment.fecha_final
        # si existe overlapping or closure no se puede ingresar
        if overlapped_ini or overlapped_end or outside_period:
            return True, f"{consignment} Conflicto con la consignacion: {c}"
    return False, f"No existe conflicto para ingresar la consignación"


class V2SRConsignments(Document):
    id_elemento = StringField(required=True, unique=True)
    desde = DateTimeField(required=False, default=None)
    hasta = DateTimeField(required=False, default=None)
    elemento = DictField(required=False)
    # consignaciones recientes is deprecated
    consignacion_reciente = EmbeddedDocumentField(V2SRConsignment, required=False, default=None)
    consignaciones = ListField(EmbeddedDocumentField(V2SRConsignment), default=[])
    document = StringField(required=True, default=V2_SR_CONSIGNMENT_LABEL)
    meta = {"collection": "INFO|Consignaciones"}

    def __init__(self, id_elemento:str, desde, hasta, *args, **values):
        super().__init__(*args, **values)
        self.id_elemento = id_elemento
        if isinstance(desde, dt.datetime) and self.desde is None:
            self.desde = desde
        if isinstance(hasta, dt.datetime) and self.hasta is None:
            self.hasta = hasta

        if isinstance(desde, str) and self.desde is None:
            self.desde = dt.datetime.strptime(desde, "%Y-%m-%d %H:%M:%S")
        if isinstance(hasta, str) and self.hasta is None:
            self.hasta = dt.datetime.strptime(hasta, "%Y-%m-%d %H:%M:%S")

        if self.consignacion_reciente is not None:
            # this field is deprecated
            self.consignacion_reciente = None

    def insert_consignment(self, consignment: V2SRConsignment):
        if consignment.fecha_inicio >  consignment.fecha_final:
            return False, f"Fechas incorrectas {consignment.fecha_inicio} > {consignment.fecha_final}"
        # si es primera consignacion a insertar
        if len(self.consignaciones) == 0:
            self.consignaciones.append(consignment)
            self.desde, self.hasta = consignment.fecha_inicio, consignment.fecha_final
            return True, f"Consignación insertada: {consignment}"
        conflict, msg = check_consignment_conflict(consignment, self.consignaciones)
        if conflict:
            return False, msg
        self.consignaciones.append(consignment)
        self.consignaciones.sort(key=lambda c: c.fecha_inicio, reverse=False)
        if len(self.consignaciones) > 0:
            self.desde, self.hasta = self.consignaciones[0].fecha_inicio, self.consignaciones[-1].fecha_final
        return True, f"Consignación insertada: {consignment}"

    def update_from_until(self, from_date: dt.datetime, until_date: dt.datetime):
        if self.hasta is None or self.desde is None:
            self.desde = from_date
            self.hasta = until_date
        else:
            if from_date < self.desde:
                self.desde = from_date
            if self.hasta < until_date:
                self.hasta = until_date

    def delete_consignment_by_id(self, consignment_id: str, remove_folder=True):
        consignment_to_delete, consignments = None, list()
        for c in self.consignaciones:
            if c.id_consignacion != consignment_id:
                consignments.append(c)
            else:
                consignment_to_delete = c

        if consignment_to_delete is None:
            return False, f"No existe la consignación [{consignment_id}] en elemento [{self.id_elemento}]"

        self.consignaciones = consignments
        if remove_folder:
            # Se procede a eliminar si existe:
            if consignment_to_delete.folder is not None and os.path.exists(consignment_to_delete.folder):
                rmtree(consignment_to_delete.folder)
        # Order consignments:
        self.consignaciones.sort(key=lambda csg: csg.fecha_inicio, reverse=False)
        if len(self.consignaciones) > 0:
            self.desde, self.hasta = self.consignaciones[0].fecha_inicio, self.consignaciones[-1].fecha_final
        return True, f"Consignación [{consignment_id}] ha sido eliminada"

    def consignments_in_time_range(self, ini_date: dt.datetime | str, end_time: dt.datetime | str) -> List[V2SRConsignment]:
        (success1, ini_date), (success2, end_time) = check_date(ini_date), check_date(end_time)

        return [c for c in self.consignaciones if
                (ini_date <= c.fecha_inicio < end_time or ini_date < c.fecha_final <= end_time) or
                # el periodo consignado cubre la totalidad del periodo a evaluar:
                (c.fecha_inicio <= ini_date and c.fecha_final >= end_time)]

    def search_consignment_by_id(self, id_to_search):
        for consignment in self.consignaciones:
            if consignment.id_consignacion == id_to_search:
                return True, consignment
        return False, None

    def edit_consignment_by_id(self, id_to_edit: str, consignment: V2SRConsignment):
        self.delete_consignment_by_id(id_to_edit, remove_folder=False)
        return self.insert_consignment(consignment)

    def __str__(self):
        return f"{self.id_elemento}: [from: {self.desde}, until: {self.hasta}] " \
               f" Total: {len(self.consignaciones)}"
