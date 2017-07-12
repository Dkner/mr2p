import redis
import pymongo
from utils.cached import Cached


class RedisConnection(metaclass=Cached):
    def __init__(self, host, port, db, password):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.connection = None

    def __enter__(self):
        if not self.connection or not self.connection.ping():
            self.connection = redis.Redis(self.host, self.port, self.db, self.password)
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class MongoConnection(metaclass=Cached):
    def __init__(self, host, port, db, user, password):
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.connection = None
        self.database = None

    def __enter__(self):
        if not self.connection:
            self.connection = pymongo.MongoClient(self.host, self.port)
            db_eval_str = "self.connection.{}".format(self.db)
            self.database = eval(db_eval_str)
            auth_ret = self.database.authenticate(self.user, self.password, self.db)
            if not auth_ret:
                raise RuntimeError('connect mongo fail')
        return self.database

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()


class ConnectionFactory(object):
    @staticmethod
    def get_redis_connection(host='127.0.0.1', port=6379, db=0, password=''):
        try:
            redis_cached = RedisConnection(host, int(port), int(db), password)
            return redis_cached
        except Exception as e:
            print(e)
            return False

    @staticmethod
    def get_mongo_connection(host='127.0.0.1', port=27017, db='', user='', password=''):
        try:
            mongo_cached = MongoConnection(host, int(port), db, user, password)
            return mongo_cached
        except Exception as e:
            print(e)
            return False