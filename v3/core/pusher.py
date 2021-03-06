import datetime
import copy
from bson import ObjectId
from core.header import *
from core.lcurl import Lcurl
from functools import wraps
from core.funcbox import FUNCBOX
from core.configuration import Configuration
import asyncio
from core.connection_factory import ConnectionFactory
from core.logger import LOG
import threading
from queue import Queue, Empty


# monitor
def stat(func):
    @wraps(func)
    def wrapper(*args, **kw):
        ret = func(*args, **kw)
        self = args[0]
        self.stat_by_redis(ret)
        return ret
    return wrapper


# write back push flag
def write_back(flag_name):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kw):
            ret = func(*args, **kw)
            if ret:
                self = args[0]
                data = args[1]
                self.write_back_mongo(ret, data, flag_name)
            return ret
        return wrapper
    return decorator


def count_second(func):
    def wrapper(*args, **kw):
        t1 = int(time.time())
        func(*args, **kw)
        t2 = int(time.time())
        print("Func name: %s\nStart time: %s\nEnd time: %s\nTime consumed: %s(s)" % (func.__name__, t1, t2, t2-t1))
    return wrapper


class Pusher(object):
    def __init__(self, job):
        # unique job name
        self.job = job

        # import data map config, such as MAP_ITORANGE
        global_config = Configuration()
        self.config = copy.deepcopy(global_config)
        self.config.import_internal_config(job)
        self.config.print_config()

        self._lcurl = Lcurl()
        self._loop = None

        self.__message_producer = None
        self.__message_consumer = None

    def trans(self, input, map, exception_default=''):
        output = {}
        func_service = FUNCBOX()
        try:
            for (k, (v, func_name, must_or_not)) in map.items():
                kw = {"value": v, "new": input, "instance": self, "method": func_name}
                try:
                    col = getattr(func_service, func_name)(**kw)
                    if not col and int(must_or_not)==1:
                        LOG.error('trans {} to column [{}] failed'.format(v, k))
                        return False
                    output[k] = col
                except Exception as e:
                    if int(must_or_not)==1:
                        LOG.error('trans {} to column [{}] failed'.format(v, k))
                        return False
                    output[k] = exception_default
            return output
        except Exception as e:
            LOG.error(e)
            return False

    async def worker(self, message_queue):
        LOG.info('Start worker for job {}'.format(self.job))

        while True:
            try:
                data = message_queue.get(block=False)
            except Empty:
                await asyncio.sleep(1)
                continue
            try:
                if data:
                    future = await self.process(data)

            except Exception as e:
                LOG.error('Error during data processing: %s' % e)
            finally:
                pass

    def process_message(self, message_queue):
        LOG.info('Start message consumer for job {}'.format(self.job))
        task_config = self.config.CONFIG['GLOBAL']['JOB'][self.job]
        processor_num = int(task_config.get('PROCESSOR_NUM', 1))

        # 事件循环
        self._loop = asyncio.new_event_loop()
        try:
            for i in range(processor_num):
                asyncio.ensure_future(coro_or_future=self.worker(message_queue), loop=self._loop)
            self._loop.run_forever()
            # self._loop.run_until_complete(asyncio.gather(self.worker(redis_conn)))
        except Exception as e:
            print(asyncio.gather(*asyncio.Task.all_tasks()).cancel(), loop=self._loop)
            # self._loop.run_until_complete(self._loop.shutdown_asyncgens())
        finally:
            self._loop.close()

    def get_message(self, message_queue):
        LOG.info('Start message producer for job {}'.format(self.job))
        task_config = self.config.CONFIG['GLOBAL']['JOB'][self.job]
        redis_schema = task_config.get('REDIS_SCHEMA', 'DEFAULT')
        redis_instance = ConnectionFactory.get_redis_connection(**self.config.CONFIG['GLOBAL']['REDIS'][redis_schema])
        with redis_instance as redis_conn:
            while True:
                if message_queue.qsize() < 10:
                    record = redis_conn.blpop(task_config['PUSH_REDIS_KEY'])
                    if record:
                        LOG.info('put message into queue: {}'.format(record))
                        data = json.loads(record[1].decode('utf-8'))
                        message_queue.put(data)
                else:
                    LOG.info('too busy, have a rest...')
                    time.sleep(1)

    def run(self):
        message_queue = Queue()
        self.__message_producer = threading.Thread(target=self.get_message, args=(message_queue,), daemon=True)
        self.__message_producer.start()
        self.__message_consumer = threading.Thread(target=self.process_message, args=(message_queue,), daemon=True)
        self.__message_consumer.start()

        self.__message_producer.join()
        self.__message_consumer.join()

    # replaced by pull_from_xxx
    def pull(self):
        pass

    # extended by son object
    def process(self, event):
        raise NotImplementedError

    @count_second
    def mongo2redis(self, skip=0):
        job_config = self.config.CONFIG['GLOBAL']['JOB'][self.job]
        redis_schema = job_config.get('REDIS_SCHEMA', 'DEFAULT')
        redis_instance = ConnectionFactory.get_redis_connection(**self.config.CONFIG['GLOBAL']['REDIS'][redis_schema])
        with redis_instance as redis_conn:
            mongo_schema = job_config.get('MONGO_SCHEMA', 'DEFAULT')
            mongo_instance = ConnectionFactory.get_mongo_connection(db=job_config['MONGO_DB'], **self.config.CONFIG['GLOBAL']['MONGO'][mongo_schema])
            with mongo_instance as db:
                mongo_collection = eval('db.{}'.format(job_config['MONGO_COLLECTION']))
                count = mongo_collection.count()
                start, step = int(skip), 50
                # company_set = set()
                while start < count:
                    print(start)
                    this_loop_records = mongo_collection.find().limit(step).skip(start)
                    for i in this_loop_records:
                        i['_id'] = str(i['_id'])
                        # if i.get('company_name','') and i['company_name'] not in company_set:
                        # if i.get('company_name',''):
                            # company_set.add(i['company_name'])
                        redis_conn.rpush(job_config['PUSH_REDIS_KEY'], json.dumps(i))
                    start += step

    def stat_by_redis(self, ret):
        redis_schema = self.config.CONFIG['GLOBAL']['JOB'][self.job].get('REDIS_SCHEMA', 'DEFAULT')
        redis_instance = ConnectionFactory.get_redis_connection(**self.config.CONFIG['GLOBAL']['REDIS'][redis_schema])
        with redis_instance as redis_conn:
            push_redis_key = self.config.CONFIG['GLOBAL']['JOB'][self.job]['PUSH_REDIS_KEY']
            stat_key = push_redis_key + '_stat_' + datetime.datetime.now().strftime("%Y-%m-%d")
            is_existed = redis_conn.hgetall(stat_key)
            if is_existed:
                if ret:
                    redis_conn.hincrby(stat_key, "success", 1)
                else:
                    redis_conn.hincrby(stat_key, "fail", 1)
                redis_conn.hset(stat_key, "last_push", int(time.time()))
            else:
                stat_map = {
                    "project": self.config.NAME,
                    "task": self.job,
                    "success": 0,
                    "fail": 0,
                    "last_push": int(time.time())
                }
                if ret:
                    stat_map['success'] = 1
                else:
                    stat_map['fail'] = 1
                redis_conn.hmset(stat_key, stat_map)

    def write_back_mongo(self, ret, data, flag_name):
        if not ret or not isinstance(data, dict) or not '_id' in data:
            return False
        job_config = self.config.CONFIG['GLOBAL']['JOB'][self.job]
        mongo_schema = job_config.get('MONGO_SCHEMA', 'DEFAULT')
        mongo_instance = ConnectionFactory.get_mongo_connection(db=job_config['MONGO_DB'], **self.config.CONFIG['GLOBAL']['MONGO'][mongo_schema])
        with mongo_instance as db:
            mongo_collection = eval('db.{}'.format(job_config['MONGO_COLLECTION']))
            ret = mongo_collection.update_one({'_id': ObjectId(data['_id'])}, {"$set": {flag_name: 1}})
            return ret

    '''
        api func start
    '''
    async def fuzzySuggestCorpName(self, session, keyword):
        if not keyword:
            return False
        url = self.config.CONFIG['GLOBAL']['API']['INTSIG_API'] + '/user/CCAppService/enterprise/advanceSearch'
        url_param = {
            'keyword': keyword,
            'start': 0
        }
        ret = await self._lcurl.get(session, url, url_param)
        if not ret:
            return False
        try:
            if ret['status'] == '1' and ret['data']['total'] > 0:
                return ret['data']['items'][0]
            else:
                return False
        except Exception as e:
            LOG.error('[fuzzySuggestCorpName ERROR] %s' % e)
            return False

    async def getSummaryByName(self, session, name):
        if not name:
            return False
        url = self.config.CONFIG['GLOBAL']['API']['CORP_QUERY_API'] + '/CCAppService/enterprise/getSummaryByName'
        url_param = {
            'name': name
        }
        ret = await self._lcurl.get(session, url, url_param)
        LOG.info('get corp summary by name[{}]...result: {}'.format(name, ret))
        if not ret:
            return False
        if str(ret['status']) == '1':
            return ret['data']
        else:
            return False

    async def download_from_camfs(self, session, filename):
        if not filename:
            return False
        url = self.config.CONFIG['GLOBAL']['API']['FILE_API'] + '/download'
        ret = await self._lcurl.get(session, url,{'filename':filename}, False, response_type='binary')
        if not ret:
            return False
        return ret

    async def upload_pic_2b(self, session, binary_data):
        if not binary_data:
            return False
        url = self.config.CONFIG['GLOBAL']['API']['COMPANY_FILE_API'] + '/common_upload_picture?type=1&user_id=11111&client_id=1001'
        ret = await self._lcurl.post(session, url, binary_data, False)
        if not ret:
            return False
        return ret['url']

    async def get_raw(self, session, url, len=0):
        if not url:
            return False
        ret = await self._lcurl.get(session=session, url=url, do_log=False, response_type='text')
        if len>0 and ret:
            ret = ret[:len]
        return ret

    async def add_ccinfo_msg_target(self, session, document):
        url = self.config.CONFIG['GLOBAL']['API']['YUNYING_PUSH_API'] + '/push/add_ccinfo_msg_target'
        ret = await self._lcurl.post(session, url, data=json.dumps(document))
        LOG.info('add ccinfo msg target by {}, result: {}'.format(document, ret))
        if ret and str(ret['errno']) == '0':
            return True
        else:
            return False
    '''
        api func end
    '''




