import json
import time
from plugins.pusher import pusher
from core.logger import LOG
from utils.tool import Tool
import asyncio
import aiohttp


class Zhaozhongbiao_total(pusher):
    def __init__(self, job):
        super().__init__(job=job)

    async def process(self, data):
        LOG.info('Zhaozhongbiao process {}'.format(data))
        async with aiohttp.ClientSession(loop=self._loop) as session:
            # 推送到启信宝易文斌
            if data.get('last_update') and int(data['last_update']) > 1477929600:
                await self.update_biddings(session, data)

    async def update_biddings(self, session, document):
        if not document:
            return False
        channel = 'HR72FESQE5TZPWDLIAY8D8EX71Z3WGNW'
        timestamp = int(time.time())
        body_str = json.dumps(document)
        sign = Tool.md5('{}{}{}'.format(body_str, channel, timestamp))
        url = '{}?timestamp={}&sign={}'.format(self.config.CONFIG['GLOBAL']['API']['RM_UPDATE_BIDDINGS_API'], timestamp, sign)
        ret = await self._lcurl.post(session=session, url=url, data=body_str, headers={"Content-Type":"application/json"})
        LOG.info('update total bidding by {}, result: {}'.format(document, ret))
        if not ret:
            return False
        if ret['status'] == 200:
            return True
        else:
            return False

