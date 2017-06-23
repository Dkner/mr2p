import sys
# import requests
from core.nlog import Nlog
is_py2 = sys.version[0] == '2'
import logging
import asyncio
import aiohttp
import async_timeout


class Lcurl(object):
    def __init__(self):
        self._logger = Nlog()

    async def get(self, session='', url='', params={}, do_log=True, headers={}, response_type='json'):
        try:
            with async_timeout.timeout(5):
                async with session.get(url, params=params, headers=headers) as r:
                    if r.status != 200:
                        return False
                    else:
                        if response_type == 'binary':
                            response = await r.read()
                        elif response_type == 'text':
                            response = await r.text()
                        else:
                            response = await r.json(content_type=None)
                        if do_log:
                            self._logger.send_api_log(url, params, response)
                        return response
        except Exception as e:
            if do_log:
                self._logger.send_api_log(url, params, e)
            logging.error(e)
            return False

    async def post(self, session='', url='', data='', do_log=True, headers={}, response_type='json'):
        try:
            with async_timeout.timeout(5):
                async with session.post(url, data=data, headers=headers) as r:
                    if r.status != 200:
                        return False
                    else:
                        if response_type == 'binary':
                            response = await r.read()
                        elif response_type == 'text':
                            response = await r.text()
                        else:
                            response = await r.json(content_type=None)
                        if do_log:
                            self._logger.send_api_log(url, data, response)
                        return response
        except Exception as e:
            if do_log:
                self._logger.send_api_log(url, data, e)
            logging.error(e)
            return False