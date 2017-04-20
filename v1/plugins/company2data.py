import json
import time
from plugins.pusher import pusher, stat
from core.lcurl import Lcurl
# import asyncio


class Company2data(pusher):

	def __init__(self, job, eventdriver):
		pusher.__init__(self, job, eventdriver)

	def pre_trans(self, data):
		if 'founding_time' in data:
			data['founding_time'] = self.change_date(data['founding_time'])
		if 'logo_url' in data:
			data['logo_url'] = self.change_url(data['logo_url'])

	def change_date(self, date):
		try:
			return int(time.mktime(time.strptime(date, '%Y.%m')))
		except Exception as e:
			return ''

	def change_url(self, url):
		binary_pic = self.download_from_camfs('10005_'+url)
		if not binary_pic:
			return ''
		else:
			url = self.upload_pic_2b(binary_pic)
			if not url:
				return ''
			else:
				return url

	@stat
	def process(self, event):
		print('company2data process')
		data = event._dict['data']
		if 'corp_category' in data and int(data['corp_category']) != 1:
			return False
		company_info = self.getSummaryByName(data.get('company_name',''))
		# company_info = None
		if not company_info or not company_info['_id']:
			company_info = {'_id':''}
		self.pre_trans(data)
		print('%s-----------%s' % (data.get('company_name',''), company_info['_id']))
		data['company_id'] = company_info['_id']
		company_increment_output = self.trans(data, self.config.CONFIG['DATA_MAP']['trans_company_increment_map'], None)
		ret = self.upload_company_increment(company_increment_output)
		return ret
		# return True

	def upload_company_increment(self, document):
		if not document:
			return False
		url = self.config.CONFIG['GLOBAL']['API']['COMPANY_INCREMENT_API']
		curl = Lcurl()
		data = {"document":document}
		r = curl.post(url=url, data=json.dumps(data), headers={"Content-Type":"application/json"})
		if not r:
			return False
		ret = r.json()
		if ret['code'] == 0:
			return True
		else:
			return False