import sys
import requests
import urllib
from core.nlog import Nlog
is_py2 = sys.version[0] == '2'

class Lcurl(object):
    def __init__(self):
        self._logger = Nlog()

    def get(self, url='', params={}, do_log=True, headers={}):
        try:
            if is_py2:
                url_params = urllib.urlencode(params)
            else:
                url_params = urllib.parse.urlencode(params)
            url = url + '?' + url_params
            r = requests.get(url=url, headers=headers, timeout=5)
            if r.status_code != 200:
                r.raise_for_status()
                return False
            else:
                if do_log:
                    if is_py2:
                        self._logger.send_api_log(url, params, r.content)
                    else:
                        self._logger.send_api_log(url, params, r.content.decode())
                return r
        except Exception as e:
            if do_log:
                self._logger.send_api_log(url, params, e)
            print(e)
            return False

    def post(self, url='', data='', do_log=True, headers={}):
        try:
            r = requests.post(url=url, data=data, headers=headers, timeout=5)
            if r.status_code != 200:
                r.raise_for_status()
                return False
            else:
                if do_log:
                    if is_py2:
                        self._logger.send_api_log(url, data, r.content)
                    else:
                        self._logger.send_api_log(url, data, r.content.decode())
                return r
        except Exception as e:
            if do_log:
                self._logger.send_api_log(url, data, e)
            print(e)
            return False