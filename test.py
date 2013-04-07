import unittest
from amongo import AMongoObject
from pymongo import MongoClient


class AMongoTest(unittest.TestCase):

    def setUp(self):
        self.conn = MongoClient()
        self.db = self.conn.test_db
        self.coll = self.db.test_coll

    def test_group(self):
        amo = AMongoObject(self.coll)
        amo.group(by=('host',), count=True)
        print amo.execute()


if __name__ == '__main__':
    unittest.main(testRunner=unittest.TextTestRunner(verbosity=2))