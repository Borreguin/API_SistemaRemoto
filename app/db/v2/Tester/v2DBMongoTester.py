import unittest

from app.db.v2.Tester.util import connect_to_test_db, connectTestDB


class V2DBMongoTester(unittest.TestCase):

    def test_db_connection(self):
        is_connected = connect_to_test_db()
        self.assertEqual(is_connected, True)

    @connectTestDB
    def test_isupper(self):
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())

    def test_split(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)


if __name__ == '__main__':
    unittest.main()
