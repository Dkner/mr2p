#coding=utf-8
import os
import sys
import time
import json
import re
import hashlib
import threading
import pymongo
import redis
import requests
from core.exception import *

is_py2 = sys.version[0] == '2'
if is_py2:
	reload(sys)
	sys.setdefaultencoding('utf-8')