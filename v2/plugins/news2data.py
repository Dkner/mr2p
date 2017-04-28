import json
import time
from plugins.pusher import pusher, stat, write_back
from core.lcurl import Lcurl
import asyncio
import aiohttp


class News2data(pusher):

	def __init__(self, job):
		pusher.__init__(self, job)

	@stat
	@write_back('2data_pushed')
	async def process(self, data):
		print('news2data process')
		if self.config.ENV == 'DEV':
			return True
		async with aiohttp.ClientSession() as session:
			news_output = self.trans(data, self.config.CONFIG['DATA_MAP'])
			ret = await self.upload_news(session, news_output)
			return ret

	async def upload_news(self, session, data):
		if not data:
			return False
		url = self.config.CONFIG['GLOBAL']['API']['BUSINESS_TOPNEWS_API']
		curl = Lcurl()
		ret = await curl.post(session=session, url=url, data=json.dumps(data), headers={"Content-Type":"application/vnd.kafka.json.v1+json"})
		# with open('data/test.log','a+') as f:
		# 	f.write(json.dumps(ret)+"\n")
		if not ret:
			return False
		if ret['code'] == 0:
			return True
		else:
			return False