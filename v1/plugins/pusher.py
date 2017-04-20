import datetime
from bson import ObjectId
from core.header import *
from core.nlog import Nlog
from core.lcurl import Lcurl
from core.funcbox import FUNCBOX
from core.configuration import Configuration
from core.egine import TheEvent

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
				data = args[1]._dict['data']
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
	def __init__(self, job, eventdriver):
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

		# event driver
		self._eventdriver = eventdriver
		self._eventdriver.add_event_listener(job, self.process)

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

	@count_second
	def run(self):
		try:
			data_gen = self.pull()
			for data in data_gen:
				# self.process(data)
				self._eventdriver.send_event(TheEvent(self.job, {'data': data}))
		except NetworkError as e:
			print('Network Error: %s' % e)
		except FinishedError as e:
			print('Data Received, processing background')
			self._eventdriver.stop_util_complete()
			print('Job Done: %s' % e)
		except InternalError as e:
			print('Internal Error: %s' % e)
		except Exception as e:
			print('Unknown Error: %s' % e)
		finally:
			pass

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
		count = 1000
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
	def get_related_corps(self, url, source):
		if not source:
			req_body = {'url':url}
		else:
			req_body = {'url':url, 'source':source}
		ret = self._lcurl.post(self.config.CONFIG['GLOBAL']['API']['TIDY_SERVER_API'] + "/tidyserver/getrelatedcorps", json.dumps(req_body), True, {"content-type":"application/json"})
		if not ret:
			return False
		try:
			ret = json.loads(ret.json())
		except Exception as e:
			print('[get_related_corps ERROR] %s' % e)
			ret = False
		if ret and not 0 == ret['error_no']:
			ret = False
		else:
			ret = ret['data']
		return ret

	def get_nace_id(self, url, source):
		if not source:
			req_body = {'url':url}
		else:
			req_body = {'url':url, 'source':source}
		ret = self._lcurl.post(self.config.CONFIG['GLOBAL']['API']['TIDY_SERVER_API'] + "/tidyserver/getnacetags", json.dumps(req_body), True, {"content-type":"application/json"})
		if not ret:
			return False
		try:
			ret = json.loads(ret.json())
		except Exception as e:
			print('[get_nace_id ERROR] %s' % e)
			return False
		if ret and not 0 == ret['error_no']:
			ret = False
		else:
			ret = ret['nace_tag']
		return ret

	def get_weixin_gzh_detail(self, src_id):
		ret = self._lcurl.post('http://ccinfo.intsig.net/ccinfo/v2/weixin_gzh', json.dumps({'weixin_gzh_id':src_id}), True)
		if not ret:
			return False
		ret = ret.json()
		return ret

	def get_hot_key_detail(self, hot_key):
		ret = self._lcurl.post('http://ccinfo.intsig.net/ccinfo/v2/hot_words', json.dumps({'word':hot_key}), True)
		if not ret:
			return False
		ret = ret.json()
		return ret

	def add_push_cnt(self, id, type):
		ret = self._lcurl.post('http://ccinfo.intsig.net/ccinfo/v2/modify_hot_words', json.dumps({'id':id, 'type':type, 'opt':"push"}), True)
		if not ret:
			return False
		ret = ret.json()
		if not ret['ret']:
			return False
		else:
			return ret

	def add_industry_push_cnt(self, industry_id):
		ret = self._lcurl.post('http://ccinfo.intsig.net/ccinfo/v2/modify_industry_tag', json.dumps({'industry_id':industry_id, 'opt':"push"}), True)
		if not ret:
			return False
		ret = ret.json()
		if not ret['ret']:
			return False
		else:
			return ret

	def add_yunying_push_target(self, post_data):
		ret = self._lcurl.post(self.config.CONFIG['GLOBAL']['API']['YUNYING_PUSH_API']+'/push/add_ccinfo_msg_target', json.dumps(post_data),True)
		if not ret:
			return False
		ret = ret.json()
		return ret

	def fuzzySuggestCorpName(self, keyword):
		if not keyword:
			return False
		url = self.config.CONFIG['GLOBAL']['API']['INTSIG_API'] + '/user/CCAppService/enterprise/advanceSearch'
		url_param = {
			'keyword': keyword,
			'start': 0
		}
		r = self._lcurl.get(url, url_param)
		if not r:
			return False
		try:
			ret = r.json()
			if ret['status'] == '1' and ret['data']['total'] > 0:
				return ret['data']['items'][0]
			else:
				return False
		except Exception as e:
			print('[fuzzySuggestCorpName ERROR] %s' % e)
			return False

	def getSummaryByName(self, name):
		if not name:
			return False
		url = self.config.CONFIG['GLOBAL']['API']['CORP_QUERY_API'] + '/CCAppService/enterprise/getSummaryByName'
		url_param = {
			'name': name
		}
		r = self._lcurl.get(url, url_param)
		if not r:
			return False
		ret = r.json()
		if ret['status'] == '1':
			return ret['data']
		else:
			return False

	def download_from_camfs(self, filename):
		if not filename:
			return False
		url = self.config.CONFIG['GLOBAL']['API']['FILE_API'] + '/download'
		r = self._lcurl.get(url,{'filename':filename}, False)
		if not r:
			return False
		return r.content

	def upload_pic_2b(self, binary_data):
		if not binary_data:
			return False
		url = self.config.CONFIG['GLOBAL']['API']['COMPANY_FILE_API'] + '/common_upload_picture?type=1&user_id=11111&client_id=1001'
		r = self._lcurl.post(url, binary_data, False)
		if not r:
			return False
		ret = r.json()
		return ret['url']
	'''
		api func end
	'''




