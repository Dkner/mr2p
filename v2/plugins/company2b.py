import json
import time
from plugins.pusher import pusher, stat, write_back
from core.lcurl import Lcurl
import aiohttp


class Company2b(pusher):

	def __init__(self, job):
		super().__init__(job=job)

	async def pre_trans(self, session, data):
		if 'logo_url' in data:
			data['logo_url'] = await self.change_url(session, data['logo_url'])
		if 'product_url' in data:
			data['product_url'] = await self.change_url(session, data['product_url'])
		if 'rival_companies' in data:
			data['rival_companies'] = await self.change_rival_companies(session, data['rival_companies'])
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

	async def change_rival_companies(self, session, opponets=[]):
		rival_companies_after = []
		for i in opponets:
			# company_info = self.fuzzySuggestCorpName(i)
			company_info = await self.getSummaryByName(session, i)
			if not company_info:
				continue
			# rival_companies_after.append(company_info['id'])
			rival_companies_after.append(company_info['_id'])
		return rival_companies_after

	@stat
	@write_back('2b_pushed')
	async def process(self, data):
		print('company2b process')
		if self.config.ENV == 'DEV':
			return True
		async with aiohttp.ClientSession(loop=self._loop) as session:
			if not 'company_name' in data or not data.get('logo_url','') or not data.get('product_url','') or ('corp_category' in data and int(data['corp_category']) != 1) or int(data.get('2b_pushed', 0))==1:
				return False
			company_info = await self.getSummaryByName(session, data['company_name'])
			if not company_info or not company_info['_id']:
				print('company id is null')
				return False
			await self.pre_trans(session, data)
			print('%s-----------%s'%(data['company_name'], company_info['_id']))
			map = self.config.CONFIG['DATA_MAP']
			corp_output = self.trans(data, map['trans_corp_map'])
			ret = await self.upload_company_extend_info(session=session, corp_id=company_info['_id'], post_data=corp_output)
			if 'trans_product_map' in map:
				product_output = self.trans(data, map['trans_product_map'])
				await self.upload_product_info(session=session, corp_id=company_info['_id'], post_data=product_output)
			return ret

	async def upload_company_extend_info(self, session, corp_id, post_data):
		if not corp_id or not post_data:
			return False
		url = self.config.CONFIG['GLOBAL']['API']['COMPANY_INFO_API'] + '/upload_company_extend_info_v2?type=200&scheme=yunying&company_id='+corp_id
		ret = await self._lcurl.post(session=session, url=url, data=json.dumps(post_data))
		if not ret:
			return False
		if ret['status'] == '1':
			return True
		else:
			return False

	async def upload_product_info(self, session, corp_id, post_data):
		if not corp_id or not post_data:
			return False
		url = self.config.CONFIG['GLOBAL']['API']['COMPANY_INFO_API'] + '/upload_product_info_v2?type=200&scheme=yunying&company_id='+corp_id
		ret = await self._lcurl.post(session=session, url=url, data=json.dumps(post_data))
		if not ret:
			return False
		if ret['status'] == '1':
			return ret['data']['product_id']
		else:
			return False