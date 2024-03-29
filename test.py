import unittest
from amongo import *
from pymongo import MongoClient
from bson.son import SON


class AMongoTest(unittest.TestCase):
    def setUp(self):
        self.conn = MongoClient()
        self.db = self.conn.test_db
        self.coll = self.db.test_coll

    def test_1(self):
        amo = self.coll.a

        actual = amo. \
            group(by=('host', 'uri'), add_count=True). \
            sort(count=-1). \
            group(by='host', count=sum_('count'), uris=push_({'uri': '$uri', 'count': '$count'})). \
            sort(('count', -1), 'host'). \
            execute()

        expected = self.coll.aggregate([
            {'$group': {'_id': {'host': '$host', 'uri': '$uri'}, 'count': {'$sum': 1}}},
            {'$project': {'_id': False, 'host': '$_id.host', 'uri': '$_id.uri', 'count': True}},

            {'$sort': {'count': -1}},

            {'$group': {'_id': '$host', 'count': {'$sum': '$count'}, 'uris': {'$push': {'uri': '$uri', 'count': '$count'}}}},
            {'$project': {'_id': False, 'host': '$_id', 'count': True, 'uris': True}},

            {'$sort': SON([('count', -1), ('host', 1)])},
        ])['result']

        # print expected[:5]

        print amo.pipeline
        # print actual[:5]
        print len(actual)

        self.assertGreater(len(actual), 0)
        # self.assertEqual(len(expected), len(actual))
        # self.assertEqual(len(actual), len(set(tuple(d.items()) for d in actual)))
        # self.assertSetEqual(set(tuple(d.items()) for d in expected), set(tuple(d.items()) for d in actual))
        self.assertListEqual(expected, actual)


if __name__ == '__main__':
    unittest.main(testRunner=unittest.TextTestRunner(verbosity=0))