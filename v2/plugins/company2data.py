import json
import time
from plugins.pusher import pusher, stat, write_back
from core.logger import LOG
import asyncio
import aiohttp


class Company2data(pusher):

    def __init__(self, job):
        super().__init__(job=job)
        if not self.config.CONFIG['DATA_MAP'].get('trans_company_increment_map', None):
            LOG.error('for job[{}], trans_company_increment_map is invalid'.format(self.job))
            raise Exception('Initial error')

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
        LOG.info('company2data [{}] process {}'.format(self.job, data))
        config = self.config
        if config.ENV == 'DEV':
            return True
        async with aiohttp.ClientSession(loop=self._loop) as session:
            if 'corp_category' in data and int(data['corp_category']) != 1:
                LOG.warning('for job[{}], corp_category is invalid in {}'.format(self.job, data))
                return False
            await self.pre_trans(session, data)
            # 上传公司(必要）
            company_increment_output = self.trans(data, config.CONFIG['DATA_MAP']['trans_company_increment_map'], None)
            await self.upload_company_increment(session, company_increment_output)

            # 上传产品（非必要）
            if not config.CONFIG['DATA_MAP'].get('trans_product_increment_map', None):
                LOG.warning('for job[{}], trans_product_increment_map is invalid'.format(self.job))
                return
            product_increment_output = self.trans(data, config.CONFIG['DATA_MAP']['trans_product_increment_map'], None)
            await self.upload_product_increment(session, product_increment_output)

    async def upload_company_increment(self, session, document):
        if not document:
            return False
        url = self.config.CONFIG['GLOBAL']['API']['COMPANY_INCREMENT_API']
        data = {
            "topic": "EnterprisesIntsigInc",
            "document": document
        }
        ret = await self._lcurl.post(session=session, url=url, data=json.dumps(data), headers={"Content-Type":"application/json"})
        LOG.info('upload company increment by {}, result: {}'.format(data, ret))
        if not ret:
            return False
        if ret['code'] == 0:
            return True
        else:
            return False

    async def upload_product_increment(self, session, document):
        if not document:
            return False
        url = self.config.CONFIG['GLOBAL']['API']['COMPANY_INCREMENT_API']
        data = {
            "topic": "ProductsIntsigInc",
            "document": document
        }
        ret = await self._lcurl.post(session=session, url=url, data=json.dumps(data), headers={"Content-Type":"application/json"})
        LOG.info('upload product increment by {}, result: {}'.format(data, ret))
        if not ret:
            return False
        if ret['code'] == 0:
            return True
        else:
            return False


class Kr2data(Company2data):
    def __init__(self, job):
        super().__init__(job=job)

    def pre_trans(self, session, data):
        if data.get('base', None):
            data['base'] = json.loads(data['base'])
            if isinstance(data['base'].get('industryTag', None), list):
                data['key_words'] = [tag['name'] for tag in data['base']['industryTag'] if tag.get('name', None)]
        if data.get('member', None):
            data['member'] = json.loads(data['member'])
            if isinstance(data['member'].get('members', None), list):
                key_trans = {
                    'name': 'per_name',
                    'position': 'per_title',
                    'intro': 'per_profile',
                }
                data['person'] = []
                for item in data['member']['members']:
                    new_item = {}
                    for key, value in key_trans.items():
                        try:
                            new_item[value] = item.pop(key)
                        except Exception as e:
                            pass
                    data['person'].append(new_item)
        if data.get('finance', None):
            data['finance'] = json.loads(data['finance'])
            if isinstance(data['finance'], list):
                key_trans = {
                    'financeAmount': 'money',
                    'financeDate': 'time',
                    'participantVos': 'Sponsor',
                    'phase': 'turn'
                }
                data['finance_history'] = []
                for item in data['finance']:
                    new_item = {}
                    for key, value in key_trans.items():
                        try:
                            new_item[value] = item.pop(key)
                        except Exception as e:
                            pass
                    data['finance_history'].append(new_item)
        if data.get('similar', None):
            data['similar'] = json.loads(data['similar'])
            if isinstance(data['similar'], list):
                data['opponents_products'] = []
                for opponent in data['similar']:
                    if isinstance(opponent['companyList'], list):
                        data['opponents_products'].extend([opponent_company['name'] for opponent_company in opponent['companyList'] if opponent_company.get('name', None)])
                data['opponents_products'] = list(set(data['opponents_products']))

    @stat
    # @write_back('2data_pushed')
    async def process(self, data):
        LOG.info('Kr2data process {}'.format(data))
        config = self.config
        async with aiohttp.ClientSession(loop=self._loop) as session:
            self.pre_trans(session, data)
            # 上传公司
            company_increment_output = self.trans(data, config.CONFIG['DATA_MAP']['trans_company_increment_map'], None)
            await self.upload_company_increment(session, company_increment_output)

            # 上传产品
            if not config.CONFIG['DATA_MAP'].get('trans_product_increment_map', None):
                LOG.warning('for job[{}], trans_product_increment_map is invalid'.format(self.job))
                return
            product_increment_output = self.trans(data, config.CONFIG['DATA_MAP']['trans_product_increment_map'], None)
            await self.upload_product_increment(session, product_increment_output)