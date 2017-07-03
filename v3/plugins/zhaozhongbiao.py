import json
import time

import aiohttp

from core.logger import LOG
from core.pusher import Pusher
from utils.tool import Tool


class Zhaozhongbiao(Pusher):
    def __init__(self, job):
        super().__init__(job=job)

    async def process(self, data):
        LOG.info('{} process {}'.format(self.job, data))
        async with aiohttp.ClientSession(loop=self._loop) as session:
            # 推送web seo到林雨森
            await self.synchronizeWeb(session, data)
            # 推送到启信宝易文斌
            await self.update_biddings(session, data)
            # 组装数据推送ccpush信息流
            target = await self.load_msg_target(session, data)
            await self.add_ccinfo_msg_target(session, target)

    def init_verify_code(self, rec_id, timestamp):
        src_code = 'IS@SHWJC_' + rec_id + '_' + str(timestamp)
        return Tool.md5(src_code)

    async def update_biddings(self, session, document):
        if not document:
            return False
        channel = 'HR72FESQE5TZPWDLIAY8D8EX71Z3WGNW'
        timestamp = int(time.time())
        body_str = json.dumps(document)
        sign = Tool.md5('{}{}{}'.format(body_str, channel, timestamp))
        url = '{}?timestamp={}&sign={}'.format(self.config.CONFIG['GLOBAL']['API']['RM_UPDATE_BIDDINGS_API'], timestamp, sign)
        ret = await self._lcurl.post(session=session, url=url, data=body_str, headers={"Content-Type":"application/json"})
        LOG.info('update bidding by {}, result: {}'.format(document, ret))
        if not ret:
            return False
        if ret['status'] == 200:
            return True
        else:
            return False

    async def synchronizeWeb(self, session, data):
        post = [{
            'info_id': str(data['_id']),
            'issue_time': data['last_update']
        }]
        ret = await self._lcurl.post(session, self.config.CONFIG['GLOBAL']['API']['WEB_API'] + "/seo/multiaddbidid", json.dumps(post))
        LOG.info('web seo result: {}'.format(ret))

    async def load_msg_target(self, session, data):
        push_corp_name, push_corp_id = None, None
        send_corps, tmp_related_corps, related_corps, related_corp_ids = [], [], [], []
        if "related_corps" in data:
            tmp_related_corps.extend(data['related_corps'])
        if "中标信息" == data['notice_type']:
            template_name = 'info_zhongbiao_new_v2'
            if "result_corps" in data and not 0 == len(data["result_corps"]):
                for corp in data['result_corps']:
                    if ',' in corp:
                        tmp_corps = corp.split(',')
                        send_corps.extend(tmp_corps)
                    else:
                        send_corps.append(corp)
        elif "招标信息" == data['notice_type']:
            template_name = 'info_zhaobiao_new_v2'
            if "tender_corp" in data and not 0 == len(data['tender_corp']):
                if ',' in data['tender_corp']:
                    tmp_corps = data['tender_corp'].split(',')
                    send_corps.extend(tmp_corps)
                else:
                    send_corps.append(data['tender_corp'])
            if "tender_agent" in data and not 0 == len(data['tender_agent']):
                if ',' in data['tender_agent']:
                    tmp_corps = data['tender_agent'].split(',')
                    send_corps.extend(tmp_corps)
                else:
                    send_corps.append(data['tender_agent'])
        else:
            return
        if 0 == len(send_corps):
            return
        for corp in send_corps:
            if corp in tmp_related_corps:
                tmp_related_corps.remove(corp)
            corp_summary = await self.getSummaryByName(session, corp)
            if corp_summary and corp_summary.get('_id'):
                if not push_corp_id:
                    push_corp_name = corp
                    push_corp_id = corp_summary.get('_id')
                else:
                    related_corps.append(corp)
                    related_corp_ids.append(corp_summary.get('_id'))
        if push_corp_name is None or push_corp_id is None:
            return
        for corp in tmp_related_corps:
            corp_summary = await self.getSummaryByName(session, corp)
            if corp_summary and corp_summary.get('_id'):
                related_corps.append(corp)
                related_corp_ids.append(corp_summary.get('_id'))
        related_corp_id_list = ','.join(related_corp_ids)
        related_corp_name_list = ','.join(related_corps)
        timestamp = int(time.time())
        post = {
            "template_name": template_name,
            "token_param": json.dumps({
                "%corp_id%": push_corp_id,
                "%corp_name%": push_corp_name,
                "%link.title%": data['title'],
                "%link.url%": "{}/personal/zhaobiao?rec_id={}&ts={}&verify_code={}".format(
                    self.config.CONFIG['GLOBAL']['API']['WEB_M_API'], data['_id'], str(timestamp),
                    self.init_verify_code(data['_id'], timestamp))
            }),
            "msg_param": json.dumps({
                "nace_code": data['nace_code'],
                "area_code": data['area_code'],
                "related_corp_ids": related_corp_id_list,
                "related_corp_names": related_corp_name_list
            }),
            "src_id": data['_id'],
            "src_channel": "spider"
        }
        return post
