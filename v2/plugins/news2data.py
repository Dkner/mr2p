import json
from plugins.pusher import pusher, stat, write_back
from core.lcurl import Lcurl
import aiohttp


class News2data(pusher):

	def __init__(self, job):
		pusher.__init__(self, job)

	async def pre_trans(self, session, data):
		if not data.get('content'):
			if data.get('link',''):
				data['html'] = await self.get_raw(session, data['link'], 30000)
			elif data.get('url',''):
				data['html'] = await self.get_raw(session, data['url'], 30000)

	@stat
	@write_back('2data_pushed')
	async def process(self, data):
		print('news2data process')
		if self.config.ENV == 'DEV':
			return True
		async with aiohttp.ClientSession() as session:
			await self.pre_trans(session, data)
			news_output = self.trans(data, self.config.CONFIG['DATA_MAP'])
			ret = await self.upload_news(session, news_output)
			return ret

	async def upload_news(self, session, document):
		if not document:
			return False
		url = self.config.CONFIG['GLOBAL']['API']['BUSINESS_TOPNEWS_API']
		curl = Lcurl()
		data = {"records":[{"value":document}]}
		ret = await curl.post(session=session, url=url, data=json.dumps(data), headers={"Content-Type":"application/vnd.kafka.json.v1+json"}, do_log=False)
		print('upload_news=========',ret)
		# with open('data/test.log','a+') as f:
		# 	f.write(json.dumps(ret)+"\n")
		if not ret:
			return False
		else:
			return True