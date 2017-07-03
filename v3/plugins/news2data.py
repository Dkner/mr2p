import json

import aiohttp

from core.logger import LOG
from core.pusher import Pusher, stat, write_back


class News2data(Pusher):
    def __init__(self, job):
        super().__init__(self, job)

    async def pre_trans(self, session, data):
        if not data.get('content'):
            if data.get('link', ''):
                data['html'] = await self.get_raw(session, data['link'], 30000)
            elif data.get('url', ''):
                data['html'] = await self.get_raw(session, data['url'], 30000)

    @stat
    @write_back('2data_pushed')
    async def process(self, data):
        print('news2data [{}] process {}'.format(self.job, data))
        if self.config.ENV == 'DEV':
            return True
        async with aiohttp.ClientSession(loop=self._loop) as session:
            await self.pre_trans(session, data)
            news_output = self.trans(data, self.config.CONFIG['DATA_MAP'])
            ret = await self.upload_news(session, news_output)
            return ret

    async def upload_news(self, session, document):
        if not document:
            return False
        url = self.config.CONFIG['GLOBAL']['API']['BUSINESS_TOPNEWS_API']
        data = {"records": [{"value": document}]}
        ret = await self._lcurl.post(session=session, url=url, data=json.dumps(data),
                                     headers={"Content-Type": "application/vnd.kafka.json.v1+json"}, do_log=False)
        LOG.info('upload news by {}, result: {}'.format(document, ret))
        if not ret:
            return False
        else:
            return True
