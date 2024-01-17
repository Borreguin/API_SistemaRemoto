from enum import Enum
from typing import Optional, List

from pydantic import BaseModel


class EntityInfoRequest(BaseModel):
    id_entidad: str
    entidad_nombre: str
    entidad_tipo: str
    activado: Optional[bool]


class NodeInfoRequest(BaseModel):
    id_node: Optional[str]
    nombre: str
    tipo: str
    activado: Optional[bool]
    entidades: List[EntityInfoRequest]


class BasicNodeInfoRequest(BaseModel):
    nombre: str
    tipo: str
    activado: Optional[bool]
    entidades: Optional[List[EntityInfoRequest]]


class NodeNewName(BaseModel):
    nuevo_nombre: str = 'Nuevo nombre'


class Option(str, Enum):
    REEMPLAZAR = 'REEMPLAZAR'
    EDIT = 'EDIT'


class RTURequest(BaseModel):
    id_utr: str = "Id UTR"
    utr_tipo: str = "tipo de UTR"
    utr_nombre: str = "Nombre UTR"
    activado: bool = True
    protocol: str = "Protocolo"
    latitude: float = 0
    longitude: float = 0


class RTURequestId(BaseModel):
    id_utr: str = 'utr id'


class TagRequest(BaseModel):
    tag_name: str = "Nombre de tag"
    filter_expression: str = "Expresión de filtro indisponibilidad"
    activado: bool = True


class TagListRequest(BaseModel):
    tags: List[TagRequest]


class EditTagRequest(BaseModel):
    tag_name: str = "Nombre editado de la tag, es el nuevo nombre de la tag"
    filter_expression: str = "Expresión de filtro indisponibilidad"
    activado: bool = True
    tag_name_original: str = "Nombre original de la tag, es el nombre de Tag a editar"


class EditedListTagRequest(BaseModel):
    tags: List[EditTagRequest]


class DeletedTagList(BaseModel):
    tags: List[str] = ["Nombre de tag"]


class TriggerRequest(BaseModel):
    hours: int = 0
    minutes: int = 0
    seconds: int = 0


class MailConfigRequest(BaseModel):
    from_email: str = "sistemaremoto@cenace.org.ec"
    users: List[str] = ["mail1@dominio.com, mail2@dominio.com"]
    admin: List[str] = ["mail1@dominio.com, mail2@dominio.com"]


class ParametersRequest(BaseModel):
    disp_utr_umbral: float = 0.95
    disp_tag_umbral: float = 0.95


class ConfigReport(BaseModel):
    trigger: TriggerRequest
    mail_config: MailConfigRequest
    parameters: ParametersRequest


class DetalleConsignacionRequest(BaseModel):
    elemento: Optional[dict] = dict(description="Descripción del elemento en formato JSON")
    no_consignacion: str = "Id de elemento"
    detalle: Optional[dict] = dict(description="json con detalle de la consignación")
    responsable: str = "responsable del ingreso de consignación"

class V2ConsignmentDetails(BaseModel):
    detalle: str
    descripcion_corta: str
    consignment_type: str
    element: dict

class V2ConsignmentRequest(BaseModel):
    no_consignacion: str
    responsable: str
    fecha_inicio: Optional[str]
    fecha_final: Optional[str]
    element_info: Optional[V2ConsignmentDetails]


class ConsignacionRequest(BaseModel):
    elemento: Optional[dict] = dict(description="Descripción del elemento en formato JSON")
    no_consignacion: str = "Id de elemento"
    fecha_inicio: str = "yyyy-mm-dd hh:mm:ss"
    fecha_final: str = "yyyy-mm-dd hh:mm:ss"
    detalle: Optional[dict] = "json con detalle de la consignación"
    responsable: str = "Responsable"


class FormatOption(str, Enum):
    EXCEL = 'excel'
    JSON = 'json'


class NodeRequest(BaseModel):
    tipo: str = "Tipo de nodo"
    nombre: str = "Nombre de nodo"
    activado: Optional[bool] = True


class NodesRequest(BaseModel):
    nodos: List[str] = ["nodo1", "nodo2", "etc"]


class GroupedOption(str, Enum):
    grouped = "agrupado"
    no_grouped = "no-agrupado"


class RoutineOptions(str, Enum):
    RUTINA_DIARIA = 'rutina_de_reporte_diario'
    RUTINA_CORREO = 'rutina_correo_electronico'


class InstallationRequest(BaseModel):
    instalacion_ems_code: str = "Código EMS de la instalación"
    instalacion_tipo: str = "Tipo de instalación"
    instalacion_nombre: str = "Nombre de instalación"
    activado: bool = True
    protocolo: str = "Protocolo"
    longitud: float = 0
    latitud: float = 0

class BahiaRequest(BaseModel):
    bahia_code: str = "Código de bahía"
    voltaje: float = 0
    bahia_nombre: str = "Nombre de bahía"
    activado: bool = True