from fastapi import APIRouter

from app.core.config import Settings
from app.endpoints.AdminConsignacion.v1.v1_AdminConsignacion import v1_admin_consignments
from app.endpoints.AdminConsignacion.v2.v2_AdminConsignacion import v2_admin_consignments

router = APIRouter(
    prefix=f"{Settings.API_PREFIX}/admin-consignacion",
    tags=["admin-consignacion"],
    responses={404: {"description": "Not found"}},
)

def create_grouped_endpoints(_router: APIRouter):
    v1_admin_consignments(_router)
    v2_admin_consignments(_router)