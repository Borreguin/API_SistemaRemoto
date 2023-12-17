
from app.common import error_log
from app.common.util import get_time_in_minutes, to_dict
from app.core.repositories import local_repositories
from app.db.constants import attributes_consignments
from app.db.v1.Info import *
from app.db.v2.entities.v2_sRConsigmentDetails import V2SRConsignmentDetails


class V2SRConsignment(EmbeddedDocument):
    consignment_id = StringField(default=None, required=True)
    no_consignacion = StringField(required=True)
    fecha_inicio = DateTimeField(required=True, default=dt.datetime.now())
    fecha_final = DateTimeField(required=True, default=dt.datetime.now())
    t_minutos = IntField(required=True)
    element_info = EmbeddedDocumentField(V2SRConsignmentDetails, required=False, default=None)
    folder = StringField(default=None, required=False)
    responsable = StringField(required=True, default=None)
    updated = DateTimeField(required=False, default=dt.datetime.now())

    def __init__(self, *args, **values):
        super().__init__(*args, **values)
        if self.consignment_id is None:
            self.consignment_id = str(uuid.uuid4())
        self.calculate()

    def create_folder(self):
        this_repo = os.path.join(local_repositories.CONSIGNMENTS, self.consignment_id)
        if not os.path.exists(this_repo):
            os.makedirs(this_repo)
            self.folder = this_repo
            return True
        return False

    def calculate(self):
        if isinstance(self.fecha_inicio, str):
            self.fecha_inicio = dt.datetime.strptime(self.fecha_inicio, "%Y-%m-%d %H:%M:%S")
        if isinstance(self.fecha_final, str):
            self.fecha_final = dt.datetime.strptime(self.fecha_final, "%Y-%m-%d %H:%M:%S")

        if self.fecha_inicio >= self.fecha_final:
            return False, "La fecha de inicio no puede ser mayor o igual a la fecha de fin"
        self.t_minutos = get_time_in_minutes(self.fecha_inicio, self.fecha_final)
        return True, f"Consignación calculada: {self}"


    def __str__(self):
        return f"({self.no_consignacion}: min={self.t_minutos}) [{self.fecha_inicio.strftime('%d-%m-%Y %H:%M')}, " \
               f"{self.fecha_final.strftime('%d-%m-%Y %H:%M')}]"

    def to_dict(self):
        return dict(no_consignacion=self.no_consignacion,
                    fecha_inicio=str(self.fecha_inicio), fecha_final=str(self.fecha_final),
                    id_consignacion=self.consignment_id, responsable=self.responsable,
                    element_info=self.element_info.to_dict() if self.element_info is not None else None)

    def edit(self, to_update: dict):
        try:
            for key, value in to_update.items():
                if key in attributes_consignments:
                    setattr(self, key, value)
            self.updated = dt.datetime.now()
            return True, f"Consignación editada"
        except Exception as e:
            msg = f"Error al actualizar {self}: {str(e)}"
            error_log.error(msg)
            return False, msg

