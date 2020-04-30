from my_lib.mongo_engine_handler import sRNode as DS
from my_lib.mongo_engine_handler import sRNodeReport as nRep
from my_lib.mongo_engine_handler import Consignment as Cons
import random as r
from mongoengine import *
import datetime as dt
from settings import initial_settings as init
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

    nodes = 4
    for n in range(nodes):
        inserting_a_SRNode(f"Test_node_{str(n+1)}")

    for n in range(nodes):
        inserting_a_SRNodeReport(f"Test_node_{str(n+1)}")

    for n in range(nodes):
        deleting_a_SRNode(f"Test_node_{str(n+1)}")

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

    nodeReport = nRep.SRNodeDetails(fecha_inicio=ini_date, fecha_final=end_date, nodo=nodo)
    # nodeReport.
    entities_report = list()
    for e in nodo.entidades:
        indisponibilidad_detalle = list()
        # Simulating the calculation of unavailable time
        for t in e.tags:
            tag_report = nRep.SRTagDetails(tag_name=t.tag_name, indisponible_minutos=r.randint(0, n_minutos_evaluate*0.15) )
            indisponibilidad_detalle.append(tag_report)

        # Getting consignments
        consignments = Cons.Consignments.objects(id=e.consignaciones.id).first()
        if consignments is None:
            return False, "No se encintraón consignaciones asociadas"
        in_period_consignments = consignments.consignments_in_time_range(ini_date, end_date)
        EntReport = nRep.SREntityDetails(id_entidad=e.id_entidad, tipo=e.tipo, nombre=e.nombre,
                                         indisponibilidad_detalle=indisponibilidad_detalle,
                                         consignaciones_detalle=in_period_consignments,
                                         periodo_evaluacion_minutos=n_minutos_evaluate)
        entities_report.append(EntReport)

    nodeReport.reportes_entidades = entities_report
    nodeReport.calculate_all()
    nodeReport.save()
    return True, "Reporte calculado éxitosamente"


def inserting_a_SRNode(node_name):
    print(f"--------- INSERTING A NEW NODE: {node_name}")
    entidades = list()

    for e in range(4):
        id_entidad = f"{node_name}_UTR_{str(e)}"
        print("\n>> -- " + id_entidad)
        # creando tags ficticias
        tags = list()
        for i in range(r.randint(0, 20)):
            tag = DS.SRTag(tag_name=f"{id_entidad}_tag_{str(i)}",
                       filter_expression="TE#ME", activado=True)
            tags.append(tag)

        entity = DS.SREntity(id_entidad=id_entidad, tipo="Subestación",
                             nombre=f"N_{id_entidad}", tags=tags)

        consignaciones = DS.Consignments.objects(id_entidad=id_entidad).first()
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
        entidades.append(entity)

    node = DS.SRNode(nombre=node_name, tipo="TEST_ONLY", entidades=entidades)
    print(f"Nodo insertado: {node}")
    node.save()

    return True


if __name__ == "__main__":
    test()