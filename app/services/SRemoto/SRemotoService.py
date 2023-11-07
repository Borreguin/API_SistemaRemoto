from flask import send_from_directory
from starlette import status
from starlette.responses import FileResponse

from app.common import configure_logger
from app.core.repositories import local_repositories
from app.schemas.RequestSchemas import FormatOption
from app.db.v1.SRFinalReport.SRFinalReportTemporal import SRFinalReportTemporal
from app.db.v1.SRFinalReport.sRFinalReportBase import *
from app.db.v1.SRFinalReport.sRFinalReportPermanente import SRFinalReportPermanente
from flask_app.my_lib.utils import *

log = configure_logger("api_sRemoto.log")


def get_descarga_calculo_en_excel(formato: FormatOption, ini_date: str = "yyyy-mm-dd H:M:S",
                                  end_date: str = "yyyy-mm-dd H:M:S"):
    success1, ini_date = check_date(ini_date)
    success2, end_date = check_date(end_date)
    if not success1 or not success2:
        msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
        # Verificando si debe usar el reporte temporal o definitivo:
    final_report_v = SRFinalReportBase(fecha_inicio=ini_date, fecha_final=end_date)
    if isTemporal(ini_date, end_date):
        final_report = SRFinalReportTemporal.objects(id_report=final_report_v.id_report).first()
    else:
        final_report = SRFinalReportPermanente.objects(id_report=final_report_v.id_report).first()
    if final_report is None:
        return dict(success=False, msg="No existe reporte asociado"), status.HTTP_404_NOT_FOUND

    utrs_dict = final_report.load_nodes_info().create_utrs_list()
    success, df_summary, df_details, df_novedades, msg = final_report.to_dataframe(utrs_dict)
    if not success:
        return dict(success=False, report=None, msg=msg), status.HTTP_409_CONFLICT

    # Creating an Excel file:
    if formato == FormatOption.EXCEL:
        ini_date_str, end_date_str = ini_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
        file_name = f"R_{ini_date_str}@{end_date_str}.xlsx"
        path = os.path.join(local_repositories.TEMPORAL, file_name)
        with pd.ExcelWriter(path) as writer:
            df_summary.to_excel(writer, sheet_name="Resumen")
            df_details.to_excel(writer, sheet_name="Detalles")
            df_novedades.to_excel(writer, sheet_name="Novedades")
        if os.path.exists(path):
            # resp = send_from_directory(os.path.dirname(path), file_name, as_attachment=True)
            # return set_max_age_to_response(resp, 5)
            return FileResponse(path=path, filename=file_name, media_type='application/octet-stream',
                                content_disposition_type="attachment"), status.HTTP_200_OK

    result_dict = dict()
    result_dict["Resumen"] = df_summary.to_dict(orient='records')
    result_dict["Detalles"] = df_details.to_dict(orient='records')
    result_dict["Novedades"] = df_novedades.to_dict(orient='records')
    return dict(success=True, report=result_dict, msg="Reporte encontrado"), status.HTTP_200_OK


def get_obtiene_listado_tags_con_indisponibilidad_mayor_igual_a_umbral(formato: FormatOption, ini_date: str = None,
                                                                       end_date: str = None, umbral=None):
    if ini_date is None or end_date is None:
        ini_date, end_date = get_last_day()
    else:
        success1, ini_date = check_date(ini_date)
        success2, end_date = check_date(end_date)
        if not success1 or not success2:
            msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
            return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    # definición del umbral
    if umbral is None:
        umbral = 0
    else:
        umbral = float(umbral)

    # Verificando si debe usar el reporte temporal o definitivo:
    final_report_virtual = SRFinalReportBase(fecha_inicio=ini_date, fecha_final=end_date)
    if isTemporal(ini_date, end_date):
        # Obtener el reporte final con los detalles de cada reporte por nodo
        final_report_db = SRFinalReportTemporal.objects(id_report=final_report_virtual.id_report).first()
    else:
        # Obtener el reporte final con los detalles de cada reporte por nodo
        final_report_db = SRFinalReportPermanente.objects(id_report=final_report_virtual.id_report).first()

    if final_report_db is None:
        return dict(success=False, msg="El reporte para esta fecha no existe. "
                                       "Considere realizar el cálculo primero"), status.HTTP_404_NOT_FOUND

    # variable para guardar el listado de tags con su respectiva indisponibilidad
    df_tag = pd.DataFrame(columns=[lb_fecha_ini, lb_fecha_fin, lb_empresa, lb_unidad_negocio, lb_utr_id, lb_utr,
                                   lb_tag_name, lb_indisponible_minutos])

    for reporte_nodo_resumen in final_report_db.reportes_nodos:
        if isTemporal(ini_date, end_date):
            reporte_nodo_db = SRNodeDetailsTemporal.objects(id_report=reporte_nodo_resumen.id_report).first()
        else:
            reporte_nodo_db = SRNodeDetailsPermanente.objects(id_report=reporte_nodo_resumen.id_report).first()
        empresa = reporte_nodo_db.nombre
        for reporte_entidad in reporte_nodo_db.reportes_entidades:
            unidad_negocio = reporte_entidad.entidad_nombre
            for reporte_utr in reporte_entidad.reportes_utrs:
                utr_id = reporte_utr.id_utr
                utr_nombre = reporte_utr.utr_nombre
                if len(reporte_utr.indisponibilidad_detalle) > 0:
                    df_tag_aux = pd.DataFrame([t.to_dict() for t in reporte_utr.indisponibilidad_detalle])
                    df_tag_aux[lb_empresa] = empresa
                    df_tag_aux[lb_unidad_negocio] = unidad_negocio
                    df_tag_aux[lb_utr_id] = utr_id
                    df_tag_aux[lb_utr] = utr_nombre
                    if umbral > 0:
                        mask = df_tag_aux[lb_indisponible_minutos] >= umbral
                        df_tag_aux = df_tag_aux[mask]
                    df_tag = df_tag.append(df_tag_aux, ignore_index=True)
    df_tag[lb_fecha_ini] = str(ini_date)
    df_tag[lb_fecha_fin] = str(end_date)
    if formato == FormatOption.JSON:
        resp = df_tag.to_dict(orient="records")
        return dict(success=True, reporte=resp), status.HTTP_200_OK

    # nombre del archivo
    ini_date_str, end_date_str = ini_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    file_name = f"IndispTags_{ini_date_str}@{end_date_str}.xlsx"
    path = os.path.join(init.TEMP_REPO, file_name)
    # crear en el directorio temporal para envío del archivo
    with pd.ExcelWriter(path) as writer:
        df_tag.to_excel(writer, sheet_name="Detalles")
    if os.path.exists(path):
        # resp = send_from_directory(os.path.dirname(path), file_name, as_attachment=True)
        # return set_max_age_to_response(resp, 2)
        return FileResponse(path=path, filename=file_name, media_type='application/octet-stream',
                            content_disposition_type="attachment"), status.HTTP_200_OK


def get_obtiene_calculo_diario(formato: FormatOption, ini_date=None, end_date=None):
    log.info("Starting this report")
    if ini_date is None and end_date is None:
        ini_date, end_date = get_dates_by_default()
    else:
        success1, ini_date = check_date_yyyy_mm_dd(ini_date)
        success2, end_date = check_date_yyyy_mm_dd(end_date)
        if not success1 or not success2:
            msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
            return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    # time range for each consult
    date_range = pd.date_range(ini_date, end_date, freq=dt.timedelta(days=1))
    if len(date_range) == 0:
        return dict(success=False, report=None, msg="Las fechas de inicio y fin no son correctas.")
    n_threads = 0
    pool = ThreadPool(8)
    results = []
    utrs_dict = None
    log.info("Going to get each report")
    for ini, end in zip(date_range, date_range[1:]):
        final_report_v = SRFinalReportTemporal(fecha_inicio=ini, fecha_final=end)
        final_report = SRFinalReportTemporal.objects(id_report=final_report_v.id_report).first()
        log.info(f"thread # {n_threads}")
        if utrs_dict is None:
            utrs_dict = final_report.load_nodes_info().create_utrs_list()
        if final_report is not None:
            results.append(pool.apply_async(final_report.to_dataframe, kwds={"utrs_dict": utrs_dict}))
            n_threads += 1
        else:
            log.warning(f"El reporte [{final_report}] no ha sido encontrado ")

    log.info(f"Se han desplegado {n_threads} threads")

    # la cola permitirá recibir los informes de manera paralela:
    df_summary, df_details, df_novedades = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    pool.close()
    pool.join()
    for result in results:
        success, _df_summary, _df_details, _df_novedades, msg = result.get()
        log.info(f"{success}, {msg}")
        if success:
            df_summary = pd.DataFrame.append(df_summary, _df_summary, ignore_index=True)
            df_details = pd.DataFrame.append(df_details, _df_details, ignore_index=True)
            df_novedades = pd.DataFrame.append(df_novedades, _df_novedades, ignore_index=True)
            n_threads -= 1
            log.info(f"threads faltantes: {n_threads}")

    # Creating an Excel file:
    if formato == FormatOption.EXCEL:
        ini_date_str, end_date_str = ini_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
        file_name = f"R_{ini_date_str}@{end_date_str}.xlsx"
        path = os.path.join(init.TEMP_REPO, file_name)
        with pd.ExcelWriter(path) as writer:
            df_summary.to_excel(writer, sheet_name="Resumen")
            df_details.to_excel(writer, sheet_name="Detalles")
            df_novedades.to_excel(writer, sheet_name="Novedades")
        if os.path.exists(path):
            resp = send_from_directory(os.path.dirname(path), file_name, as_attachment=True)
            return set_max_age_to_response(resp, 2)

    result_dict = dict()
    result_dict["Resumen"] = df_summary.to_dict(orient='records')
    result_dict["Detalles"] = df_details.to_dict(orient='records')
    result_dict["Novedades"] = df_novedades.to_dict(orient='records')
    return dict(success=True, report=result_dict, msg="Reporte encontrado")


def get_obtiene_tendencia_reporte_diario(formato: FormatOption, ini_date=None, end_date=None):
    success1, ini_date = check_date_yyyy_mm_dd(ini_date)
    success2, end_date = check_date_yyyy_mm_dd(end_date)
    if not success1 or not success2:
        msg = "No se puede convertir. " + (ini_date if not success1 else end_date)
        return dict(success=False, msg=msg), status.HTTP_400_BAD_REQUEST
    # time range for each consult
    date_range = pd.date_range(ini_date, end_date, freq=dt.timedelta(days=1))
    if len(date_range) == 0:
        return (dict(success=False, report=None, msg="Las fechas de inicio y fin no son correctas."),
                status.HTTP_400_BAD_REQUEST)

    dates = []
    values = []
    for ini, end in zip(date_range, date_range[1:]):
        final_report_v = SRFinalReportTemporal(fecha_inicio=ini, fecha_final=end)
        final_report = SRFinalReportTemporal.objects(id_report=final_report_v.id_report).first()
        dates.append(str(ini))
        if final_report is not None:
            values.append(final_report.disponibilidad_promedio_porcentage)
        else:
            values.append(None)

    if formato == FormatOption.JSON:
        result = dict(dates=dates, values=values)
        return dict(success=True, result=result, msg="Tendencia"), status.HTTP_200_OK
