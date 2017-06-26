import json
import time
from core.logger import LOG
from collections import defaultdict

import requests

from utils.singleton import Singleton

# from conf.config_demo import DEMO

CONFIG_API = {
    "DEV": "http://www.devccinfo.com/ccinfo/v2/select?type=r2m_local_conf",
    "TEST": "http://ccinfo-test.intsig.net/ccinfo/v2/select?type=r2m_local_conf",
    "ONLINE": "http://ccinfo-test.intsig.net/ccinfo/v2/select?type=r2m_local_conf"
}


class Configuration(metaclass=Singleton):

    def __init__(self):
        self.ENV = ''
        self.NAME = ''
        self.CONFIG = defaultdict(list)

    # set interval timer to update the config
    def update_config(self):
        while 1:
            try:
                # update global config
                r = requests.post(url=CONFIG_API[self.ENV], data=json.dumps({"name": self.NAME}))
                ret = r.json()
                if isinstance(ret, dict) and ret['count']>0:
                    self.CONFIG['GLOBAL'] = ret['list'][-1]['conf']
                else:
                    LOG.error('[CONFIG ERROR] global config update fail')
                # update internal config
                r = requests.post(url=CONFIG_API[self.ENV], data=json.dumps({"name": self.CONFIG['GLOBAL']["JOB"][self.JOB]['TRANS_MAP']}))
                ret = r.json()
                if isinstance(ret, dict) and ret['count'] > 0:
                    self.CONFIG['DATA_MAP'] = ret['list'][-1]['conf']
                else:
                    LOG.error('[CONFIG ERROR] internal config update error')
            except Exception as e:
                LOG.error('[CONFIG ERROR] config update error: %s' % e)
            time.sleep(10)

    def import_global_config(self, env):
        self.ENV = env
        self.NAME = 'MR2P_' + env
        self.CONFIG = {}
        try:
            r = requests.post(CONFIG_API[env], data=json.dumps({"name": self.NAME}))
            ret = r.json()
            if isinstance(ret, dict) and ret['count']>0:
                self.CONFIG['GLOBAL'] = ret['list'][-1]['conf']
                # self.CONFIG['GLOBAL']['update_time'] = ret['list'][-1]['update_time']
            else:
                LOG.error('[CONFIG ERROR] global config import error')
                exit(-1)
        except Exception as e:
            LOG.error('[CONFIG ERROR] global config import: %s'%e)
            exit(-1)

    def import_internal_config(self, job_name):
        self.JOB = job_name
        try:
            r = requests.post(CONFIG_API[self.ENV], data=json.dumps({"name": self.CONFIG['GLOBAL']["JOB"][job_name]['TRANS_MAP']}))
            ret = r.json()
            if isinstance(ret, dict) and ret['count']>0:
                self.CONFIG['DATA_MAP'] = ret['list'][-1]['conf']
                # self.CONFIG['DATA_MAP']['update_time'] = ret['list'][-1]['update_time']
            else:
                LOG.error('[ERROR] internal config import error')
                exit(-1)
        except Exception as e:
            LOG.error('[CONFIG ERROR] internal config import: %s' % e)
            exit(-1)

    def import_demo_config(self, env):
        self.ENV = env
        self.NAME = 'MR2P_' + env
        # self.CONFIG = DEMO
        self.JOB = ''

    def print_config(self):
        LOG.info("[ENVIRONMENT] %s\n[NAME] %s\n[JOB] %s\n[CONFIG] %s" % (self.ENV, self.NAME, self.JOB, self.CONFIG))

# if __name__ == '__main__':
#     import sys, getopt
#     optlist, args = getopt.getopt(sys.argv[1:], 'e:', ['env='])
#     env = ''
#     for k, v in optlist:
#         if k == '-e' or k == '--env':
#             env = v
#     test = Configuration()
#     test.import_global_config(env)
#     test.import_internal_config('ITORANGE')
#     # test.import_demo_config(env)
