import unittest
from conf.config_demo import DEMO
from core.connection_factory import ConnectionFactory


class MyTestCase(unittest.TestCase):
    def test_get_redis_connection(self):
        conn_id1, conn_id2 = None, None
        schema = DEMO.get('REDIS')
        print(schema)
        redis_instance1 = ConnectionFactory.get_redis_connection(**schema)
        with redis_instance1 as redis_conn:
            print(redis_conn.set('foo', 'bar'))
            conn_id1 = redis_conn
        redis_instance2 = ConnectionFactory.get_redis_connection(**schema)
        with redis_instance2 as redis_conn:
            print(redis_conn.get('foo'))
            conn_id2 = redis_conn
        print(redis_instance1, id(conn_id1), redis_instance2, id(conn_id2))
        self.assertEqual(id(conn_id1), id(conn_id2))

    def test_get_mongo_connection(self):
        conn_id1, conn_id2 = None, None
        schema = DEMO.get('MONGO')
        print(schema)
        mongo_instance1 = ConnectionFactory.get_mongo_connection(db='d_ccinfo', **schema)
        with mongo_instance1 as db:
            print(db.zhaobiaozhongbiao.find_one())
            conn_id1 = mongo_instance1.connection
        mongo_instance2 = ConnectionFactory.get_mongo_connection(db='d_ccinfo', **schema)
        with mongo_instance2 as db:
            print(db.zhaobiaozhongbiao.find_one())
            conn_id2 = mongo_instance2.connection
        print(mongo_instance1, id(conn_id1), mongo_instance2, id(conn_id2))
        self.assertEqual(id(conn_id1), id(conn_id2))

if __name__ == '__main__':
    unittest.main()
