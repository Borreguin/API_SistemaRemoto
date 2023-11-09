import unittest

from app.db.v2.Tester.util import *
from app.db.v2.util import save_mongo_document_safely

empty_nodes = 1
with_entities = 2


class V2DBMongoTester(unittest.TestCase):

    def test_db_connection(self):
        is_connected = connect_to_test_db()
        print(f"Connected: {is_connected}")
        self.assertEqual(is_connected, True)

    @connectTestDB
    def test_create_empty_node(self):
        v2_node = create_new_node(empty_nodes)
        success, msg = v2_node.save_safely()
        if not success:
            print(msg)
        self.assertEqual(True, success)

    @connectTestDB
    def test_create_node_with_entityties(self):
        v2_node = create_new_node(with_entities)
        v2_node.entidades = [create_new_entity(with_entities + 1), create_new_entity(with_entities + 2)]
        success, msg = save_mongo_document_safely(v2_node)
        if not success:
            print(msg)
        self.assertEqual(True, success)

    @connectTestDB
    def test_delete_empty_node(self):
        deleted = delete_node(empty_nodes)
        self.assertEqual("Deleted", deleted)

    @connectTestDB
    def test_delete_node_with_entities(self):
        deleted = delete_node(with_entities)
        self.assertEqual("Deleted", deleted)


if __name__ == '__main__':
    unittest.main()
