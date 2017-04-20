import datetime
from bson import ObjectId
from core.header import *
from core.nlog import Nlog
from core.lcurl import Lcurl
from core.funcbox import FUNCBOX
from core.configuration import Configuration
import asyncio

POP_TIMEOUT = 60

# monitor
def stat(func):
	def wrapper(*args, **kw):
		ret = func(*args, **kw)
		self = args[0]
		self.stat_by_redis(ret)
		return ret
	return wrapper

# write back push flag
def write_back(flag_name):
	def decorator(func):
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


class pusher(object):
	def __init__(self, job):
		# unique job name
		self.job = job

		# import data map config, such as MAP_ITORANGE
		self.config = Configuration()
		self.config.import_internal_config(job)
		self.config.print_config()

		# refresh config
		# config_listener = threading.Thread(target=self.config.update_config)
		# config_listener.setDaemon(True)
		# config_listener.start()

		self._lcurl = Lcurl()
		self._logger = Nlog()

		try:
			pull_strategy = self.config.CONFIG['GLOBAL']['JOB'][job]['PULL_STRATEGY']
			print('[PULL STRATEGY] %s' % pull_strategy)
			if pull_strategy is not None:
				pull_method = eval('self.pull_from_' + pull_strategy)
				setattr(self, 'pull', pull_method)
		except Exception as e:
			raise InternalError('[pusher constructor ERROR]', e)

	# data pull strategy
	def pull_from_mongo(self):
		(mongo_client, mongo_database) = self.connect_mongo(self.config.CONFIG['GLOBAL']['JOB'][self.job]['MONGO_DB'])
		if not mongo_client:
			raise NetworkError("cannot connect to mongo server")
		mongo_collection = eval('mongo_database.' + self.config.CONFIG['GLOBAL']['JOB'][self.job]['MONGO_COLLECTION'])
		# total number to process
		count = mongo_collection.count()
		# loop start and loop range, related to the memory consumed
		start, step = 0, 10
		while start < count:
			this_loop_records = mongo_collection.find().limit(step).skip(start)
			for i in this_loop_records:
				yield i
			start += step
		mongo_client.close()
		raise FinishedError('finished')

	def pull_from_redis(self):
		push_redis_key = self.config.CONFIG['GLOBAL']['JOB'][self.job]['PUSH_REDIS_KEY']
		redis_conn = self.connect_redis()
		if not redis_conn or not redis_conn.ping():
			raise NetworkError("cannot connect to redis server")
		while True:
			record = redis_conn.blpop(push_redis_key, POP_TIMEOUT)
			if record is None:
				print('redis time out')
				continue
				# raise FinishedError('finished')
			data = json.loads(record[1])
			yield data

	def connect_mongo(self, dbname):
		mongo_config = self.config.CONFIG['GLOBAL']['MONGO']
		if not mongo_config or not 'host' in mongo_config or not 'port' in mongo_config or not 'user' in mongo_config or not 'password' in mongo_config:
			return False
		conn = pymongo.MongoClient(mongo_config['host'], int(mongo_config['port']))
		db = eval("conn."+dbname)
		ret = db.authenticate(mongo_config['user'], mongo_config['password'], dbname)
		if False == ret:
			return (conn, False)
		return (conn, db)

	def connect_redis(self):
		redis_config = self.config.CONFIG['GLOBAL']['REDIS']
		if not redis_config or not 'host' in redis_config or not 'port' in redis_config or not 'db' in redis_config:
			return False
		connection = redis.Redis(host=redis_config['host'], port=int(redis_config['port']), db=int(redis_config['db']), password=redis_config['password'])
		return connection

	def trans(self, input, map, exception_default=''):
		output = {}
		func_service = FUNCBOX()
		try:
			for (k, (v, func_name)) in map.items():
				kw = {"value": v, "new": input, "instance": self, "method": func_name}
				try:
					col = getattr(func_service, func_name)(**kw)
					output[k] = col
				except Exception as e:
					output[k] = exception_default
					# print('[Trans %s Error] %s' % (k,e))
			return output
		except Exception as e:
			print(e)
			return False

	async def worker(self, redis_conn):
		print('Start worker')
		push_redis_key = self.config.CONFIG['GLOBAL']['JOB'][self.job]['PUSH_REDIS_KEY']

		while True:
			record = redis_conn.lpop(push_redis_key)
			if record is None:
				await asyncio.sleep(1)
				continue
			data = json.loads(record.decode('utf-8'))
			try:
				await self.process(data)
			except Exception as e:
				print('Unknown Error: %s' % e)
			finally:
				pass

	def run(self):
		redis_conn = self.connect_redis()
		if not redis_conn or not redis_conn.ping():
			raise NetworkError("cannot connect to redis server")
		task_config = self.config.CONFIG['GLOBAL']['JOB']
		processor_num = int(task_config[self.job].get('PROCESSOR_NUM', 1))
		for i in range(processor_num):
			asyncio.ensure_future(self.worker(redis_conn))
		loop = asyncio.get_event_loop()
		try:
			loop.run_forever()
		except KeyboardInterrupt as e:
			print(asyncio.gather(*asyncio.Task.all_tasks()).cancel())
			# loop.run_until_complete(loop.shutdown_asyncgens())
		finally:
			loop.close()


	# replaced by pull_from_xxx
	def pull(self):
		pass

	# extended by son object
	def process(self, event):
		print(event)
		return True

	@count_second
	def mongo2redis(self):
		job_config = self.config.CONFIG['GLOBAL']['JOB'][self.job]
		redis_conn = self.connect_redis()
		(mongo_client, mongo_database) = self.connect_mongo(job_config['MONGO_DB'])
		mongo_collection = eval('mongo_database.' + job_config['MONGO_COLLECTION'])
		count = mongo_collection.count()
		count = 100
		start, step = 0, 10
		while start < count:
			print(start)
			this_loop_records = mongo_collection.find().limit(step).skip(start)
			for i in this_loop_records:
				i['_id'] = str(i['_id'])
				redis_conn.rpush(job_config['PUSH_REDIS_KEY'], json.dumps(i))
			start += step
		mongo_client.close()

	def stat_by_redis(self, ret):
		redis_conn = self.connect_redis()
		if not redis_conn or not redis_conn.ping():
			print("cannot connect to redis server")
			return False
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
		return True

	def write_back_mongo(self, ret, data, flag_name):
		if not ret or not isinstance(data, dict) or not '_id' in data:
			return False
		job_config = self.config.CONFIG['GLOBAL']['JOB'][self.job]
		(mongo_client, mongo_database) = self.connect_mongo(job_config['MONGO_DB'])
		mongo_collection = eval('mongo_database.' + job_config['MONGO_COLLECTION'])
		ret = mongo_collection.update_one({'_id': ObjectId(data['_id'])}, {"$set": {flag_name: 1}})
		mongo_client.close()
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
			print('[fuzzySuggestCorpName ERROR] %s' % e)
			return False

	async def getSummaryByName(self, session, name):
		if not name:
			return False
		url = self.config.CONFIG['GLOBAL']['API']['CORP_QUERY_API'] + '/CCAppService/enterprise/getSummaryByName'
		url_param = {
			'name': name
		}
		ret = await self._lcurl.get(session, url, url_param)
		if not ret:
			return False
		if ret['status'] == '1':
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
	'''
		api func end
	'''




