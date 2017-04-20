import sys
import socket
import struct
import time
import json
from core.configuration import Configuration
is_py2 = sys.version[0] == '2'


class Nlog(object):

	def __init__(self):
		config = Configuration().CONFIG['GLOBAL']
		self._ip = config['NLOG']['ip']
		self._port = int(config['NLOG']['port'])


	def send_api_log(self, url, post_body, result):
		return self.send_udp_log(content='[API] '+url+' '+json.dumps(post_body)+' | '+json.dumps(result))

	def send_udp_log(self, content, log_time=time.time()):
		ifname = "eth0"
		dst_ip = self._ip
		dst_port = self._port
		s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		try:
			import fcntl
			if is_py2:
				inet = fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]))
			else:
				inet = fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', bytes(ifname[:15], 'utf-8')))
			ip = socket.inet_ntoa(inet[20:24])
			addr = (dst_ip, dst_port)
			this_log = "%s [%s->%s] msg: %s\n" %(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(log_time)), ip, dst_ip, str(content))
			if is_py2:
				ret = s.sendto(this_log, addr)
			else:
				ret = s.sendto(bytes(this_log, 'utf-8'), addr)
		except Exception as e:
			print(e)
			return False
		s.close()
		return True
