from fastapi import APIRouter, UploadFile
from starlette.responses import Response

from app.services.AdminConsignacion.v2.v2_AdminConsignacionService import *


def v2_admin_consignments(router: APIRouter):
    router.tags = ["v2-admin-consignacion"]

    consignacion_elemento_fecha_uri = '/v2/consignacion/{element_id}/{ini_date}/{end_date}'
    @router.get(consignacion_elemento_fecha_uri)
    def v2_obtener_consignaciones_asociadas_elemento_id_por_fecha(element_id: str = "id_elemento",
                                                                  ini_date: str = "yyyy-mm-dd hh:mm:ss",
                                                                  end_date: str = "yyyy-mm-dd hh:mm:ss",
                                                                  response: Response = Response()):
        """ Obtener las consignaciones asociadas del elemento: "id_elemento" \n
            <b>id_elemento</b> corresponde al elemento a consignar \n
            formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
        """
        resp, response.status_code = (
            v2_get_obtener_consignaciones_asociadas_elemento_id_por_fecha(element_id, ini_date, end_date)
        )
        return resp


    @router.post(consignacion_elemento_fecha_uri)
    def v2_consignar_elemento_asociado_id_elemento(element_id: str, ini_date: str,
                                                   end_date: str,
                                                   data_request: V2ConsignmentRequest,
                                                   response: Response = Response()):
        """ Consignar un elemento asociadas a: "id_elemento" \n
            <b>id_elemento</b> corresponde al elemento a consignar \n
             formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
        """
        resp, response.status_code = (
            v2_post_consignar_elemento_asociado_id_elemento(element_id, ini_date, end_date, data_request)
        )
        return resp


    consignacion_by_elemento_and_consignacion_id = '/v2/consignacion/{element_id}/{consignment_id}'


    @router.delete(consignacion_by_elemento_and_consignacion_id)
    def v2_elimina_consignacion_por_id_elemento_id_consignacion(element_id: str = "id_elemento",
                                                             consignment_id: str = "id_consignacion",
                                                             response: Response = Response()):
        """ Elimina la consignación asociadas del elemento: "id_elemento" cuya idenficación es "id_consignacion" \n
            <b>id_elemento</b> corresponde al elemento consignado \n
            <b>id_consignacion</b> corresponde a la identificación de la consignación
        """
        resp, response.status_code = (
            v2_delete_elimina_consignacion_por_id_elemento_id_consignacion(element_id, consignment_id)
        )
        return resp


    @router.put(consignacion_by_elemento_and_consignacion_id)
    def v2_edita_consignacion_por_elemento_id_y_consignacion_id(element_id: str,
                                                                consignment_id: str,
                                                                request_data: V2ConsignmentRequest,
                                                                response: Response = Response()):
        """ Edita la consignación asociada al elemento: "id_elemento", cuya idenficación es "id_consignacion" \n
            <b>id_elemento</b> corresponde al elemento consignado \n
            <b>id_consignacion</b> corresponde a la identificación de la consignación \n
            formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
        """
        resp, response.status_code = (
            v2_put_edita_consignacion_por_elemento_id_y_consignacion_id(element_id, consignment_id, request_data)
        )
        return resp


    @router.post(consignacion_by_elemento_and_consignacion_id)
    async def v2_carga_archivo_a_consignacion_asociada_al_elemento(element_id: str,
                                                                consignment_id: str,
                                                                upload_file: UploadFile,
                                                                response: Response = Response()):
        """ Carga un archivo a la consignación asociada al elemento: "id_elemento", cuya idenficación es "id_consignacion" \n
            <b>id_elemento</b> corresponde al elemento consignado \n
            <b>id_consignacion</b> corresponde a la identificación de la consignación \n
            formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
        """
        resp, response.status_code = (
            await v2_post_carga_archivo_a_consignacion_asociada_al_elemento(element_id, consignment_id, upload_file)
        )
        return resp


    consignacion_en_formato_rango_fecha = '/v2/consignaciones/{formato}/{ini_date}/{end_date}'
    @router.get(consignacion_en_formato_rango_fecha)
    def v2_obtener_consignaciones_en_rango_fecha(formato: FormatOption = None, ini_date: str = "yyyy-mm-dd hh:mm:ss",
                                              end_date: str = "yyyy-mm-dd hh:mm:ss", response: Response = Response()):
        """ Obtener las consignaciones existentes en las fechas consultadas \n
            formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b> \n
            formatos disponibles: [json, excel]
        """
        resp, response.status_code = (
            v2_get_obtener_consignaciones_en_rango_fecha(formato, ini_date, end_date)
        )
        return resp
