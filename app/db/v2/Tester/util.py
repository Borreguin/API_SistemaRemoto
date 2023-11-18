from typing import Union, Tuple, Any, List

from faker import Faker
from faker.providers import DynamicProvider
from mongoengine import connect
from app.core.config import settings
from app.db.v2.entities.v2_sRBahia import V2SRBahia
from app.db.v2.entities.v2_sREntity import V2SREntity
from app.db.v2.entities.v2_sRInstallation import V2SRInstallation
from app.db.v2.entities.v2_sRNode import V2SRNode
from app.db.v2.entities.v2_sRTag import V2SRTag

MONGO_TESTER_DB = "DB_DISP_EMS_TEST"

installation_type_provider = DynamicProvider(
    provider_name="installation_type",
    elements=["central", "subestacion"],
)

entity_type_provider = DynamicProvider(
    provider_name="entity_type",
    elements=["unidad de negocio", "empresa", "oficina"],
)

node_type_provider = DynamicProvider(
    provider_name="node_type",
    elements=["empresa", "test", "fake"],
)


def create_fake_gen(instance) -> Faker:
    Faker.seed(instance)
    fake_gen = Faker()
    fake_gen.add_provider(node_type_provider)
    fake_gen.add_provider(entity_type_provider)
    fake_gen.add_provider(installation_type_provider)
    # fake_gen.seed(instance)
    return fake_gen


def connect_to_test_db() -> bool:
    try:
        # warnings.filterwarnings("ignore", category=DeprecationWarning)
        connect(db=MONGO_TESTER_DB, host=settings.MONGO_SERVER_IP, port=int(settings.MONGO_PORT),
                alias='default', uuidRepresentation='standard')
        return True
    except Exception as e:
        print(f"Not able to connect due to: {e}")
        return False


def connectTestDB(func):
    def wrapper(*args, **kwargs):
        connect_to_test_db()
        func(*args, **kwargs)

    return wrapper


def search_node(tipo: str, nombre: str) -> Union[tuple[bool, None], tuple[bool, V2SRNode]]:
    v2_node = V2SRNode.find(tipo, nombre)
    if v2_node is None:
        print(f"Node not found: {tipo} {nombre}")
        return False, None
    return True, v2_node


def save_node_safely(v2_node: V2SRNode) -> bool:
    success, msg = v2_node.save_safely()
    if not success:
        print(msg)
    return success


def create_new_entity(instance) -> V2SREntity:
    fake_gen = create_fake_gen(instance)
    return V2SREntity(fake_gen.entity_type(), fake_gen.first_name())


def create_new_node(instance) -> V2SRNode:
    fake_gen = create_fake_gen(instance)
    node_type, node_name = fake_gen.node_type(), fake_gen.first_name()
    print(f"Creating node: {node_type} {node_name}")
    return V2SRNode(node_type, node_name)


def create_new_bahia(instance) -> V2SRBahia:
    fake_gen = create_fake_gen(instance)
    return V2SRBahia(fake_gen.first_name(), fake_gen.last_name(), fake_gen.unique.random_int(min=13, max=500))


def create_new_installation(instance) -> V2SRInstallation:
    fake_gen = create_fake_gen(instance)
    installation_type, installation_name = fake_gen.installation_type(), fake_gen.first_name()
    ems_code = installation_type + installation_name
    v2_installation = V2SRInstallation(ems_code, installation_type, installation_name)
    success, msg = v2_installation.save_safely()
    print(msg) if not success else print(f"Created installation: {v2_installation}")
    return v2_installation


def create_new_installation_with_bahias(instance, n_bahias: int) -> V2SRInstallation:
    v2_installation = create_new_installation(instance)
    v2_installation.bahias = [create_new_bahia(f"{n}_{instance}") for n in range(n_bahias)]
    v2_installation.save_safely()
    print(f"Add bahias: {[b for b in v2_installation.bahias]}")
    return v2_installation


def create_new_entity_with_installations(instance, n_installations: int) -> V2SREntity:
    v2_entity = create_new_entity(instance)
    for n in range(n_installations):
        installation = create_new_installation(instance + v2_entity.entidad_nombre + f"{n}")
        success, msg = installation.save_safely()
        if success:
            v2_entity.instalaciones.append(installation)
    return v2_entity


def create_new_entity_with_installations_and_bahias(instance, n_installations: int, n_bahias: int) -> V2SREntity:
    v2_entity = create_new_entity(instance)
    for n in range(n_installations):
        instance = f"{n}_{instance}_{v2_entity.id_entidad}"
        installation = create_new_installation_with_bahias(instance, n_bahias)
        success, msg = installation.save_safely()
        if not success:
            print(msg)
        v2_entity.instalaciones.append(installation)
    print(f"New entity: {v2_entity}")
    return v2_entity


def create_new_installation_with_bahias_and_tags(instance, n_bahias: int, n_tags: int) -> V2SRInstallation:
    v2_installation = create_new_installation(instance)
    v2_installation.bahias = create_bahias_with_tags(instance, n_bahias, n_tags)
    v2_installation.save_safely()
    print(f"Add bahias: {[b for b in v2_installation.bahias]}")
    return v2_installation


def create_bahias_with_tags(instance, n_bahias: int, n_tags: int) -> list[V2SRBahia]:
    bahias = []
    for n in range(n_bahias):
        bahia = create_new_bahia(f"{n}-{instance}")
        bahia.tags = [create_new_tag(instance + f"{n}_{bahia.bahia_code}") for n in range(n_tags)]
        bahias.append(bahia)
    return bahias


def create_new_tag(instance) -> V2SRTag:
    fake_gen = create_fake_gen(instance)
    return V2SRTag(fake_gen.first_name())


def create_new_entity_with_bahias_and_tags(instance, n_bahias: int, n_tags: int) -> V2SREntity:
    v2_entity = create_new_entity(instance)
    installation = create_new_installation_with_bahias_and_tags(instance, n_bahias, n_tags)
    success, msg = installation.save_safely()
    if not success:
        print(msg)
    print(f"New installation: {installation}")
    v2_entity.instalaciones.append(installation)
    return v2_entity


def delete_node(instance) -> bool:
    gen = create_fake_gen(instance)
    tipo, nombre = gen.node_type(), gen.first_name()
    v2_node = V2SRNode.find(tipo, nombre)
    if isinstance(v2_node, V2SRNode):
        v2_node.delete_deeply()
        print(f"Deleted node: {tipo} {nombre}")
        return True
    print(f"No deleted node: {tipo} {nombre}")
    return False
