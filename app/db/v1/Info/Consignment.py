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
from typing import List

from app.db.v1.Info import *
from app.utils.utils import check_date


class Consignment(EmbeddedDocument):
    no_consignacion = StringField(required=True)
    fecha_inicio = DateTimeField(required=True, default=dt.datetime.now())
    fecha_final = DateTimeField(required=True, default=dt.datetime.now())
    t_minutos = IntField(required=True)
    id_consignacion = StringField(default=None, required=True)
    detalle = DictField()
    folder = StringField(default=None, required=False)
    responsable = StringField(required=True, default=None)
    updated = DateTimeField(required=False, default=dt.datetime.now())

    def __init__(self, *args, **values):
        super().__init__(*args, **values)
        if isinstance(self.fecha_inicio, str):
            self.fecha_inicio = dt.datetime.strptime(self.fecha_inicio, "%Y-%m-%d %H:%M:%S")
        if isinstance(self.fecha_final, str):
            self.fecha_final = dt.datetime.strptime(self.fecha_final, "%Y-%m-%d %H:%M:%S")
        if self.id_consignacion is None:
            # print(self.no_consignacion)
            if self.no_consignacion is None:
                return
            id = str(uuid.uuid4()) + str(self.fecha_inicio) + str(self.fecha_final)
            self.id_consignacion = hashlib.md5(id.encode()).hexdigest()
        self.calculate()

    def create_folder(self):
        this_repo = os.path.join(init.CONS_REPO, self.id_consignacion)
        if not os.path.exists(this_repo):
            os.makedirs(this_repo)
            self.folder = this_repo
            return True
        return False

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
        self.t_minutos = t.days * (60 * 24) + t.seconds // 60 + (t.seconds % 60) / 60

    def __str__(self):
        return f"({self.no_consignacion}: min={self.t_minutos}) [{self.fecha_inicio.strftime('%d-%m-%Y %H:%M')}, " \
               f"{self.fecha_final.strftime('%d-%m-%Y %H:%M')}]"

    def to_dict(self, as_str=True):
        return dict(no_consignacion=self.no_consignacion,
                    fecha_inicio=str(self.fecha_inicio) if as_str else self.fecha_inicio,
                    fecha_final=str(self.fecha_final) if as_str else self.fecha_final,
                    id_consignacion=self.id_consignacion, responsable=self.responsable,
                    detalle=self.detalle)

    def edit(self, new_consignment: dict):
        try:
            to_update = ["no_consignacion", "fecha_inicio", "fecha_final", "detalle", "responsable"]
            for key, value in new_consignment.items():
                if key in to_update:
                    setattr(self, key, value)
            self.updated = dt.datetime.now()
            return True, f"Consignación editada"
        except Exception as e:
            msg = f"Error al actualizar {self}: {str(e)}"
            tb = traceback.format_exc()  # Registra últimos pasos antes del error
            return False, msg


class Consignments(Document):
    id_elemento = StringField(required=True, unique=True)
    desde = DateTimeField(required=False, default=None)
    hasta = DateTimeField(required=False, default=None)
    elemento = DictField(required=False)
    consignacion_reciente = EmbeddedDocumentField(Consignment)
    consignaciones = ListField(EmbeddedDocumentField(Consignment))
    meta = {"collection": "INFO|Consignaciones"}

    def update_last_consignment(self):
        t, ixr = dt.datetime(1900, 1, 1), -1
        for ix, c in enumerate(self.consignaciones):
            # check last date:
            if c.fecha_final > t:
                t, ixr = c.fecha_final, ix
        if ixr != -1:
            self.consignacion_reciente = self.consignaciones[ixr]
        else:
            self.consignacion_reciente = None

    def insert_consignments(self, consignacion: Consignment):
        # si es primera consignacion a insertar
        if len(self.consignaciones) == 0:
            self.consignaciones.append(consignacion)
            self.update_last_consignment()
            self.update_from_until(consignacion.fecha_inicio, consignacion.fecha_final)
            return True, f"Consignación insertada: {consignacion}"
        where = 0
        until_date, from_date = dt.datetime(1900, 1, 1), dt.datetime(3000, 1, 1)
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
            # evaluar desde y hasta que fecha existe registro:
            if c.fecha_inicio < from_date:
                from_date = c.fecha_inicio
            if c.fecha_final > until_date:
                until_date = c.fecha_final
        if 0 <= where < len(self.consignaciones):
            self.consignaciones = self.consignaciones[0:where] + [consignacion] + self.consignaciones[where:]
        else:
            self.consignaciones.append(consignacion)
        self.desde, self.hasta = from_date, until_date
        self.update_from_until(consignacion.fecha_inicio, consignacion.fecha_final)
        self.update_last_consignment()
        return True, f"Consignación insertada: {consignacion}"

    def update_from_until(self, from_date: dt.datetime, until_date: dt.datetime):
        if self.hasta is None or self.desde is None:
            self.desde = from_date
            self.hasta = until_date
        else:
            if from_date < self.desde:
                self.desde = from_date
            if self.hasta < until_date:
                self.hasta = until_date

    def delete_consignment_by_id(self, id_consignacion):
        t_max, t_min, ixr = dt.datetime(1900, 1, 1), dt.datetime(3000, 1, 1), -1
        new_consignaciones = []
        self.consignacion_reciente = None
        for consignment in self.consignaciones:
            # Se procede a eliminar si existe:
            if consignment.id_consignacion == id_consignacion:
                if consignment.folder is not None and os.path.exists(consignment.folder):
                    rmtree(consignment.folder)
                continue
            # añadiendo elemento con id diferente:
            new_consignaciones.append(consignment)
            # check last date:
            if consignment.fecha_final > t_max:
                t_max = consignment.fecha_final
                self.ingreso_reciente = consignment
                self.hasta = t_max
            if consignment.fecha_inicio < t_min:
                t_min = consignment.fecha_inicio
                self.desde = t_min

        if len(new_consignaciones) == len(self.consignaciones):
            return False, f"No existe la consignación [{id_consignacion}] en elemento [{self.id_elemento}]"
        self.consignaciones = new_consignaciones
        return True, f"Consignación [{id_consignacion}] ha sido eliminada"

    def consignments_in_time_range(self, ini_date: dt.datetime, end_time: dt.datetime) -> List[Consignment]:
        success, ini_date = check_date(ini_date)
        success, end_time = check_date(end_time)

        return [c for c in self.consignaciones if
                (ini_date <= c.fecha_inicio < end_time or ini_date < c.fecha_final <= end_time) or
                # el periodo consignado cubre la totalidad del periodo a evaluar:
                (c.fecha_inicio <= ini_date and c.fecha_final >= end_time)]

    def get_standard_element(self):
        if "entidad_nombre" in dict(self.elemento).keys() and "utr_nombre" in dict(self.elemento).keys():
            return dict(entidad=dict(self.elemento).get("entidad_nombre"),
                        elemento=dict(self.elemento).get("utr_nombre"))
        # Caso para componentes sistema central:
        comp_path = os.path.join(init.flask_path, "dto", "mongo_engine_handler", "Components")
        if os.path.exists(comp_path):
            from flask_app.dto.mongo_engine_handler.Components.Comp_Root import ComponenteRoot
            if dict(self.elemento).get("document") == "ComponenteLeaf":
                parent_id = dict(self.elemento).get("parent_id", "")
                componente = ComponenteRoot.objects(public_id=parent_id).first()
                if componente is None:
                    return dict(entidad=None, elemento=dict(self.elemento).get("name"))
                return dict(entidad=componente.name, elemento=dict(self.elemento).get("name"))

        return dict(entidad=None, elemento=None)

    def consignments_in_time_range_w_element(self, ini_date: dt.datetime, end_time: dt.datetime):
        consignment_list = self.consignments_in_time_range(ini_date, end_time)
        result = list()
        if len(consignment_list) == 0:
            return consignment_list
        for consignment in consignment_list:
            consignment_dict = consignment.to_dict()
            detalle = consignment_dict.pop("detalle", None)
            consignment_dict.update(detalle)
            consignment_dict.update(self.get_standard_element())
            result.append(consignment_dict)
        return result

    def search_consignment_by_id(self, id_to_search):
        for consignment in self.consignaciones:
            if consignment.id_consignacion == id_to_search:
                return True, consignment
        return False, None

    def get_periodos_in_range(self, ini_date, end_date) -> (bool, List[tuple]):
        consignacion_list = self.consignments_in_time_range(ini_date, end_date)
        period_list = list()
        for consignment in consignacion_list:
            ini_per, end_per = consignment.fecha_inicio, consignment.fecha_final
            if consignment.fecha_inicio < ini_date:
                ini_per = ini_date
            if consignment.fecha_final > end_date:
                end_per = end_date
            period_list.append((ini_per, end_per))
        return len(period_list) > 0, period_list

    def edit_consignment_by_id(self, id_to_edit, consignment: Consignment):
        found = False
        for consignacion in self.consignaciones:
            if consignacion.id_consignacion == id_to_edit:
                found = True
                consignacion.edit(consignment.to_dict(as_str=False))
                break
        return found, f"La consignación {consignment.no_consignacion} ha sido editada correctamente" if found \
            else f"La consignación no ha sido encontrada"

    def __str__(self):
        return f"{self.id_elemento}: [last: {self.consignacion_reciente}] " \
               f" Total: {len(self.consignaciones)}"
