import unittest

from app.db.v2.Tester.util import *
from app.db.v2.util import save_mongo_document_safely

empty_nodes = "empty node"
node_with_entities = "empty node with entities"
nodes_with_installations = "node with installations"
node_with_installations_and_bahias = "node installations bahias"

cases = [empty_nodes, node_with_entities, nodes_with_installations, node_with_installations_and_bahias]


class V2DBMongoTester(unittest.TestCase):

    def test_db_connection(self):
        is_connected = connect_to_test_db()
        self.assertEqual(True, is_connected, "Not connected")

    @connectTestDB
    def test_create_empty_node(self):
        v2_node = create_new_node(empty_nodes)
        success = False
        if save_node_safely(v2_node):
            success, node = search_node(v2_node.tipo, v2_node.nombre)
        self.assertEqual(True, success, f"No new node was created: {empty_nodes}")

    @connectTestDB
    def test_create_node_with_entities(self):
        v2_node = create_new_node(node_with_entities)
        v2_node.entidades = [create_new_entity(node_with_entities + "1"),
                             create_new_entity(node_with_entities + "2")]
        success = False
        if save_node_safely(v2_node):
            success, node = search_node(v2_node.tipo, v2_node.nombre)
        self.assertEqual(True, success, f"No new node was created: {node_with_entities}")

    @connectTestDB
    def test_create_empty_node_with_installations(self):
        v2_node = create_new_node(nodes_with_installations)
        new_entity1 = create_new_entity_with_installations(nodes_with_installations + "1", 2)
        new_entity2 = create_new_entity_with_installations(nodes_with_installations + "2", 2)
        v2_node.entidades = [new_entity1, new_entity2]
        success = False
        if save_node_safely(v2_node):
            success, node = search_node(v2_node.tipo, v2_node.nombre)
        self.assertEqual(True, success,
                         f"No new node was created with empty installation: {nodes_with_installations}")

    @connectTestDB
    def test_create_node_with_installation_and_bahias(self):
        v2_node = create_new_node(node_with_installations_and_bahias)
        new_entity1 = create_new_entity_with_installations_and_bahias("1" + node_with_installations_and_bahias, 1, 2)
        new_entity2 = create_new_entity_with_installations_and_bahias("2" + node_with_installations_and_bahias, 1, 3)
        v2_node.entidades = [new_entity1, new_entity2]
        save_node_safely(v2_node)
        success, node = search_node(v2_node.tipo, v2_node.nombre)
        self.assertEqual(True, success,
                         f"No new node was created with empty installation: {node_with_installations_and_bahias}")

    @connectTestDB
    def test_delete_nodes(self):
        n_deleted = 0
        for case in cases:
            print(f"Deleting: {case}")
            n_deleted += 1 if delete_node(case) else 0
        self.assertEqual(len(cases), n_deleted, f"No all nodes were deleted: {cases}")


if __name__ == '__main__':
    unittest.main()
