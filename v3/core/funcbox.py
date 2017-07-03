import time
import json
import re


class FUNCBOX(object):

    def __getattr__(self, key):
        return self.my_defined_func

    def my_default_func(self, **kw):
        return kw['value']

    def my_eval_func(self, **kw):
        new = kw['new']
        return eval(kw['value'])

    def my_json_decode_func(self, **kw):
        return json.loads(kw['value'])

    def my_time_func(self, **kw):
        timestamp_str = str(int(round(time.time())))
        return timestamp_str

    def my_milltime_func(self, **kw):
        milltimestamp_str = str(int(round(time.time() * 1000)))
        return milltimestamp_str

    def my_trans_key_words_func(self, **kw):
        new = kw['new']
        value = eval(kw['value'])
        value = re.sub('(^\[|\]$)', '', value)
        return value.split(',')

    def my_defined_func(self, **kw):
        the_instance = kw['instance']
        the_method = kw['method']
        return getattr(the_instance, the_method)(**kw)