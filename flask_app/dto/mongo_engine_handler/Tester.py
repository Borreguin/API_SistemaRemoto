from flask_app.dto.mongo_engine_handler import sRNode as DS, Consignment as Cons
import random as r
from mongoengine import *
import datetime as dt

from flask_app.dto.mongo_engine_handler.SRNodeReport.SRNodeReportTemporal import SRNodeDetailsTemporal
from flask_app.dto.mongo_engine_handler.SRNodeReport.sRNodeReportBase import SREntityDetails, SRUTRDetails, SRTagDetails
from flask_app.dto.mongo_engine_handler.SRNodeReport.sRNodeReportPermanente import SRNodeDetailsPermanente
from flask_app.settings import initial_settings as init
from flask_app.my_lib.utils import isTemporal
DEBUG = True

d_n = dt.datetime.now()
ini_date = dt.datetime(year=d_n.year, month=d_n.month-1, day=1)
end_date = dt.datetime(year=d_n.year, month=d_n.month, day=d_n.day) - dt.timedelta(days=d_n.day)
t_delta = end_date - ini_date
n_minutos_evaluate = t_delta.days*(60*24) + t_delta.seconds//60 + t_delta.seconds % 60



def test():
    mongo_config = init.MONGOCLIENT_SETTINGS
    mongo_config.update(dict(db="DB_DISP_EMS_TEST"))
    connect(**mongo_config)

    nodes = 2
    for n in range(nodes):
        inserting_a_SRNode(f"N_Test{str(n+1)}")

    for n in range(nodes):
        inserting_a_SRNodeReport(f"N_Test{str(n+1)}")

    for n in range(nodes):
        deleting_a_SRNode(f"N_Test{str(n+1)}")

    disconnect()


def deleting_a_SRNode(node_name):
    nodo = DS.SRNode.objects(nombre=node_name).first()
    if nodo is not None:
        nodo.delete_all()


def inserting_a_SRNodeReport(node_name):

    print(f">> REPORT NODE: {node_name}")
    print(f"From: {ini_date.strftime('%d-%m-%Y')} to: {end_date.strftime('%d-%m-%Y')}")
    print(f"Minutes = {n_minutos_evaluate}")
    nodo = DS.SRNode.objects(nombre=node_name).first()
    if nodo is None:
        return False
    if isTemporal(ini_date, end_date):
        nodeReport = SRNodeDetailsTemporal(fecha_inicio=ini_date, fecha_final=end_date, nodo=nodo)
    else:
        nodeReport = SRNodeDetailsPermanente(fecha_inicio=ini_date, fecha_final=end_date, nodo=nodo)
    # nodeReport.
    entities_report = list()
    for e in nodo.entidades:
        # Reporte por entidad
        entity_report = SREntityDetails(entidad_nombre=e.entidad_nombre, entidad_tipo=e.entidad_tipo)
        for u in e.utrs:
            utr_report = SRUTRDetails(id_utr=u.id_utr, utr_nombre=u.utr_nombre, utr_tipo=u.utr_tipo)
            # Simulating the calculation of unavailable time
            for t in u.tags:
                tag_report = SRTagDetails(tag_name=t.tag_name,
                                               indisponible_minutos=r.randint(0, int(n_minutos_evaluate*0.005)))
                utr_report.indisponibilidad_detalle.append(tag_report)

            # Getting consignments
            consignments = Cons.Consignments.objects(id=u.consignaciones.id).first()
            if consignments is not None:
                utr_report.consignaciones_detalle = consignments.consignments_in_time_range(ini_date, end_date)
            utr_report.periodo_evaluacion_minutos = nodeReport.periodo_evaluacion_minutos
            utr_report.calculate()
            if utr_report.numero_tags > 0:
                entity_report.reportes_utrs.append(utr_report)

        # calcular el reporte de entidad
        entity_report.periodo_evaluacion_minutos = nodeReport.periodo_evaluacion_minutos
        entity_report.calculate()
        nodeReport.reportes_entidades.append(entity_report)

    # calcular el reporte final
    nodeReport.calculate_all()
    nodeReport.save()
    return True, "Reporte calculado éxitosamente"


def inserting_a_SRNode(node_name):
    print(f"--------- INSERTING A NEW NODE: {node_name}")
    # entidades a insertar
    entidades = list()
    for e in range(4):
        id_entidad = f"{node_name}_entity_{str(e)}"
        print("\n>> -- " + id_entidad)
        entity = DS.SREntity(id_entidad=id_entidad, entidad_tipo="Subestación",
                             entidad_nombre=f"N_{id_entidad}")

        # utrs a insertar:
        for u in range(4):
            id_utr = id_entidad + "_" + str(u)
            utr = DS.SRUTR(id_utr=id_utr, utr_tipo="test", utr_nombre=f"{node_name}UTR_{e}_{u}")
            # creando tags ficticias
            for i in range(r.randint(0, 20)):
                tag = DS.SRTag(tag_name=f"{id_entidad}_u{str(u)}_tag_{str(i)}",
                           filter_expression="TE#ME", activado=True)
                utr.tags.append(tag)

            consignaciones = DS.Consignments.objects(id_entidad=id_utr).first()
            # creando consignaciones
            for c in range(5):
                n_days = r.uniform(1, 30)
                t_ini_consig = end_date - dt.timedelta(days=n_days)
                t_end_consig = t_ini_consig + dt.timedelta(days=r.uniform(0, 4))
                consignacion = Cons.Consignment(fecha_inicio=t_ini_consig, fecha_final=t_end_consig,
                                                no_consignacion=str(r.randint(1, 1000)))
                # insertando la consignación
                print(consignaciones.insert_consignments(consignacion))
                # [print(c) for c in consignaciones.consignaciones]
            consignaciones.save()
            entity.utrs.append(utr)
        entidades.append(entity)

    node = DS.SRNode(nombre=node_name, tipo="TEST_ONLY", entidades=entidades)
    print(f"Nodo insertado: {node}")
    node.save()

    return True


if __name__ == "__main__":
    test()