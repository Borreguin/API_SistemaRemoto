from faker import Faker
from faker.providers import DynamicProvider
from mongoengine import connect
from app.core.config import settings
from app.db.constants import V2_SR_NODE_LABEL
from app.db.v2.entities.v2_sREntity import V2SREntity
from app.db.v2.entities.v2_sRNode import V2SRNode

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
        print(">>>>>	Connecting to test database... ")
        connect_to_test_db()
        func(*args, **kwargs)

    return wrapper


def create_new_entity(instance) -> V2SREntity:
    fake_gen = create_fake_gen(instance)
    return V2SREntity(fake_gen.entity_type(), fake_gen.first_name())


def create_new_node(instance) -> V2SRNode:
    fake_gen = create_fake_gen(instance)
    return V2SRNode(fake_gen.node_type(), fake_gen.first_name())


def delete_node(instance) -> str:
    gen = create_fake_gen(instance)
    tipo, nombre = gen.node_type(), gen.first_name()
    v2_node = V2SRNode.objects(tipo=tipo, nombre=nombre, document=V2_SR_NODE_LABEL).first()
    if isinstance(v2_node, V2SRNode):
        v2_node.delete()
        return "Deleted"
    return "No deleted"
