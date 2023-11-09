import unittest

from app.db.v2.Tester.util import *
from app.db.v2.util import save_mongo_document_safely

empty_nodes = "empty node"
with_entities = "with entities"


class V2DBMongoTester(unittest.TestCase):

    def test_db_connection(self):
        is_connected = connect_to_test_db()
        self.assertEqual(True, is_connected, "Not connected")

    @connectTestDB
    def test_create_empty_node(self):
        v2_node = create_new_node(empty_nodes)
        success, msg = v2_node.save_safely()
        if not success:
            print(msg)
        self.assertEqual(True, success, f"No new node was created: {empty_nodes}")

    @connectTestDB
    def test_create_node_with_entities(self):
        v2_node = create_new_node(with_entities)
        v2_node.entidades = [create_new_entity(with_entities + "1"), create_new_entity(with_entities + "2")]
        success, msg = save_mongo_document_safely(v2_node)
        if not success:
            print(msg)
        self.assertEqual(True, success, f"No new node was created: {with_entities}")

    @connectTestDB
    def test_delete_empty_node(self):
        deleted = delete_node(empty_nodes)
        self.assertEqual("Deleted", deleted, f"Node was not deleted: {empty_nodes}")

    @connectTestDB
    def test_delete_node_with_entities(self):
        deleted = delete_node(with_entities)
        self.assertEqual("Deleted", deleted, f"Node was not deleted: {with_entities}")


if __name__ == '__main__':
    unittest.main()
