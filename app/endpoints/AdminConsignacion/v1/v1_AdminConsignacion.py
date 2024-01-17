from fastapi import APIRouter, UploadFile
from starlette.responses import Response

from app.core.config import Settings
from app.services.AdminConsignacion.v1.v1_AdminConsignacionService import *

def v1_admin_consignments(router: APIRouter):
    router.tags = ["v1-admin-consignacion"]

    consignacion_elemento_fecha_uri = '/consignacion/{id_elemento}/{ini_date}/{end_date}'
    @router.get(consignacion_elemento_fecha_uri)
    def obtener_consignaciones_asociadas_elemento_id_por_fecha(id_elemento: str = "id_elemento",
                                                               ini_date: str = "yyyy-mm-dd hh:mm:ss",
                                                               end_date: str = "yyyy-mm-dd hh:mm:ss",
                                                               response: Response = Response()):
        """ Obtener las consignaciones asociadas del elemento: "id_elemento" \n
            <b>id_elemento</b> corresponde al elemento a consignar \n
            formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
        """
        resp, response.status_code = (
            get_obtener_consignaciones_asociadas_elemento_id_por_fecha(id_elemento, ini_date, end_date)
        )
        return resp


    @router.post(consignacion_elemento_fecha_uri)
    def consignar_elemento_asociado_id_elemento(id_elemento: str = "id_elemento", ini_date: str = "yyyy-mm-dd hh:mm:ss",
                                                end_date: str = "yyyy-mm-dd hh:mm:ss",
                                                data_request: DetalleConsignacionRequest = DetalleConsignacionRequest(),
                                                response: Response = Response()):
        """ Consignar un elemento asociadas a: "id_elemento" \n
            <b>id_elemento</b> corresponde al elemento a consignar \n
             formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
        """
        resp, response.status_code = (
            post_consignar_elemento_asociado_id_elemento(id_elemento, ini_date, end_date, data_request)
        )
        return resp


    consignacion_by_elemento_and_consignacion_id = '/consignacion/{id_elemento}/{id_consignacion}'


    @router.delete(consignacion_by_elemento_and_consignacion_id)
    def elimina_consignacion_por_id_elemento_id_consignacion(id_elemento: str = "id_elemento",
                                                             id_consignacion: str = "id_consignacion",
                                                             response: Response = Response()):
        """ Elimina la consignación asociadas del elemento: "id_elemento" cuya idenficación es "id_consignacion" \n
            <b>id_elemento</b> corresponde al elemento consignado \n
            <b>id_consignacion</b> corresponde a la identificación de la consignación
        """
        resp, response.status_code = (
            delete_elimina_consignacion_por_id_elemento_id_consignacion(id_elemento, id_consignacion)
        )
        return resp


    @router.put(consignacion_by_elemento_and_consignacion_id)
    def edita_consignacion_por_elemento_id_y_consignacion_id(id_elemento: str = "id_elemento",
                                                             id_consignacion: str = "id_consignacion",
                                                             request_data: ConsignacionRequest = ConsignacionRequest(),
                                                             response: Response = Response()):
        """ Edita la consignación asociada al elemento: "id_elemento", cuya idenficación es "id_consignacion" \n
            <b>id_elemento</b> corresponde al elemento consignado \n
            <b>id_consignacion</b> corresponde a la identificación de la consignación \n
            formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
        """
        resp, response.status_code = (
            put_edita_consignacion_por_elemento_id_y_consignacion_id(id_elemento, id_consignacion, request_data)
        )
        return resp


    @router.post(consignacion_by_elemento_and_consignacion_id)
    async def carga_archivo_a_consignacion_asociada_al_elemento(id_elemento: str,
                                                                id_consignacion: str,
                                                                upload_file: UploadFile,
                                                                response: Response = Response()):
        """ Carga un archivo a la consignación asociada al elemento: "id_elemento", cuya idenficación es "id_consignacion" \n
            <b>id_elemento</b> corresponde al elemento consignado \n
            <b>id_consignacion</b> corresponde a la identificación de la consignación \n
            formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b>
        """
        resp, response.status_code = (
            post_carga_archivo_a_consignacion_asociada_al_elemento(id_elemento, id_consignacion, upload_file)
        )
        return resp


    consignacion_en_formato_rango_fecha = '/consignaciones/{formato}/{ini_date}/{end_date}'


    @router.get(consignacion_en_formato_rango_fecha)
    def obtener_consignaciones_en_rango_fecha(formato: FormatOption = None, ini_date: str = "yyyy-mm-dd hh:mm:ss",
                                              end_date: str = "yyyy-mm-dd hh:mm:ss", response: Response = Response()):
        """ Obtener las consignaciones existentes en las fechas consultadas \n
            formato de fechas: <b>yyyy-mm-dd hh:mm:ss</b> \n
            formatos disponibles: [json, excel]
        """
        resp, response.status_code = (
            get_obtener_consignaciones_en_rango_fecha(formato, ini_date, end_date)
        )
        return resp
