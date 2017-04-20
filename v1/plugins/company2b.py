import json
import time
from plugins.pusher import pusher, stat, write_back
from core.lcurl import Lcurl
# import asyncio


class Company2b(pusher):

	def __init__(self, job, eventdriver):
		pusher.__init__(self, job, eventdriver)

	def pre_trans(self, data):
		if 'logo_url' in data:
			data['logo_url'] = self.change_url(data['logo_url'])
		if 'product_url' in data:
			data['product_url'] = self.change_url(data['product_url'])
		if 'rival_companies' in data:
			data['rival_companies'] = self.change_rival_companies(data['rival_companies'])
		if not 'multi_content' in data:
			multi_content = []
			if 'industries' in data and data['industries']:
				multi_content.append({'type':'1','content':data['industries']})
			if 'logo_url' in data and data['logo_url']:
				multi_content.append({'type':'2','content':data['logo_url']})
			if 'product_url' in data and data['product_url']:
				multi_content.append({'type':'2','content':data['product_url']})
			if 'product_description' in data and data['product_description']:
				multi_content.append({'type':'3','content':data['product_description']})
			data['multi_content'] = multi_content

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

	def change_rival_companies(self, opponets=[]):
		rival_companies_after = []
		for i in opponets:
			# company_info = self.fuzzySuggestCorpName(i)
			company_info = self.getSummaryByName(i)
			if not company_info:
				continue
			# rival_companies_after.append(company_info['id'])
			rival_companies_after.append(company_info['_id'])
		return rival_companies_after

	@stat
	@write_back('2b_pushed')
	def process(self, event):
		print('company2b process')
		data = event._dict['data']
		if not 'company_name' in data or not data.get('logo_url','') or not data.get('product_url','') or ('corp_category' in data and int(data['corp_category']) != 1) or int(data.get('2b_pushed', 0))==1:
			return False
		company_info = self.getSummaryByName(data['company_name'])
		if not company_info or not company_info['_id']:
			print('company id is null')
			return False
		self.pre_trans(data)
		print('%s-----------%s'%(data['company_name'], company_info['_id']))
		map = self.config.CONFIG['DATA_MAP']
		corp_output = self.trans(data, map['trans_corp_map'])
		ret = self.upload_company_extend_info(corp_id=company_info['_id'], post_data=corp_output)
		if 'trans_product_map' in map:
			product_output = self.trans(data, map['trans_product_map'])
			self.upload_product_info(corp_id=company_info['_id'], post_data=product_output)
		return ret

	def upload_company_extend_info(self, corp_id, post_data):
		if not corp_id or not post_data:
			return False
		url = self.config.CONFIG['GLOBAL']['API']['COMPANY_INFO_API'] + '/upload_company_extend_info_v2?type=200&scheme=yunying&company_id='+corp_id
		curl = Lcurl()
		r = curl.post(url=url, data=json.dumps(post_data))
		if not r:
			return False
		ret = r.json()
		if ret['status'] == '1':
			return True
		else:
			return False

	def upload_product_info(self, corp_id, post_data):
		if not corp_id or not post_data:
			return False
		url = self.config.CONFIG['GLOBAL']['API']['COMPANY_INFO_API'] + '/upload_product_info_v2?type=200&scheme=yunying&company_id='+corp_id
		curl = Lcurl()
		r = curl.post(url=url, data=json.dumps(post_data))
		if not r:
			return False
		ret = r.json()
		if ret['status'] == '1':
			return ret['data']['product_id']
		else:
			return False