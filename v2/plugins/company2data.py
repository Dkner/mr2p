import json
import time
from plugins.pusher import pusher, stat, write_back
from core.lcurl import Lcurl
import asyncio
import aiohttp


class Company2data(pusher):

	def __init__(self, job):
		pusher.__init__(self, job)

	async def pre_trans(self, session, data):
		if 'founding_time' in data:
			data['founding_time'] = self.change_date(data['founding_time'])
		if 'logo_url' in data:
			data['logo_url'] = await self.change_url(session, data['logo_url'])

	def change_date(self, date):
		try:
			return int(time.mktime(time.strptime(date, '%Y.%m')))
		except Exception as e:
			return ''

	async def change_url(self, session, url):
		binary_pic = await self.download_from_camfs(session, '10005_'+url)
		if not binary_pic:
			return ''
		else:
			url = await self.upload_pic_2b(session, binary_pic)
			if not url:
				return ''
			else:
				return url

	@stat
	@write_back('2data_pushed')
	async def process(self, data):
		print('company2data process')
		if self.config.ENV == 'DEV':
			return True
		async with aiohttp.ClientSession() as session:
			if 'corp_category' in data and int(data['corp_category']) != 1:
				return False
			company_info = await self.getSummaryByName(session, data.get('company_name',''))
			# company_info = None
			if not company_info or not company_info['_id']:
				company_info = {'_id':''}
			await self.pre_trans(session, data)
			print('%s-----------%s' % (data.get('company_name',''), company_info['_id']))
			data['company_id'] = company_info['_id']
			company_increment_output = self.trans(data, self.config.CONFIG['DATA_MAP']['trans_company_increment_map'], None)
			ret = await self.upload_company_increment(session, company_increment_output)
			return ret
			# return True

	async def upload_company_increment(self, session, document):
		if not document:
			return False
		url = self.config.CONFIG['GLOBAL']['API']['COMPANY_INCREMENT_API']
		curl = Lcurl()
		data = {"document":document}
		ret = await curl.post(session=session, url=url, data=json.dumps(data), headers={"Content-Type":"application/json"})
		# with open('data/test.log','a+') as f:
		# 	f.write(json.dumps(ret)+"\n")
		if not ret:
			return False
		if ret['code'] == 0:
			return True
		else:
			return False