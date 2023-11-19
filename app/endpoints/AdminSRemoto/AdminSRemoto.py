from fastapi import APIRouter
from starlette.responses import Response

from app.core.config import Settings

from app.endpoints.AdminSRemoto.RTU import rtu_endpoints
from app.endpoints.AdminSRemoto.Tags import tags_endpoints
from app.endpoints.AdminSRemoto.v1.v1_Excel import v1_node_from_excel_endpoints
from app.endpoints.AdminSRemoto.v1.v1_NodeById import v1_node_id_endpoints
from app.endpoints.AdminSRemoto.v1.v1_NodeByTypeAndName import v1_node_type_and_name_endpoints
from app.endpoints.AdminSRemoto.v1.v1_SRemoto import v1_admin_sremoto_endpoints
from app.endpoints.AdminSRemoto.v2.v2_Excel import v2_node_from_excel_endpoints
from app.endpoints.AdminSRemoto.v2.v2_NodeById import v2_node_id_endpoints
from app.endpoints.AdminSRemoto.v2.v2_NodeByTypeAndName import v2_node_type_and_name_endpoints
from app.endpoints.AdminSRemoto.v2.v2_SRemoto import v2_admin_sremoto_endpoints

router = APIRouter(
    prefix=f"{Settings.API_PREFIX}/admin-sRemoto",
    tags=["admin-sRemoto"],
    responses={404: {"description": "Not found"}},
)


def create_grouped_endpoints(_router: APIRouter):
    v1_admin_sremoto_endpoints(_router)
    v2_admin_sremoto_endpoints(_router)
    v1_node_id_endpoints(_router)
    v2_node_id_endpoints(_router)
    v1_node_type_and_name_endpoints(_router)
    v2_node_type_and_name_endpoints(_router)
    v1_node_from_excel_endpoints(_router)
    v2_node_from_excel_endpoints(_router)
    rtu_endpoints(_router)
    tags_endpoints(router)





