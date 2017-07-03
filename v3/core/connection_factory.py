import redis
import pymongo
from utils.cached import Cached


class RedisConnection(metaclass=Cached):
    def __init__(self, host, port, db, password):
        self.connection = redis.Redis(host, port, db, password)
        if not self.connection.ping():
            self.connection = False


class MongoConnection(metaclass=Cached):
    def __init__(self, host, port, db, user, password):
        self.connection = pymongo.MongoClient(host, port)
        db_eval_str = "self.connection.{}".format(db)
        database = eval(db_eval_str)
        auth_ret = database.authenticate(user, password, db)
        if not auth_ret:
            self.db = False
        else:
            self.db = database


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