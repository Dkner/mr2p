import multiprocessing
import os
import time
import threading
from core.configuration import Configuration

for filename in os.listdir(os.path.split(os.path.realpath(__file__))[0] + "/../plugins"):
	if filename[-3:] == ".py" and filename[0:-3] != "__init__":
		exec("from plugins import " + filename[0:-3])


class scheduler(object):
	def __init__(self, name, job_list):
		self.config = Configuration()
		# refresh config
		config_listener = threading.Thread(target=self.config.update_config)
		config_listener.setDaemon(True)
		config_listener.start()

		# job_name => job(subclass of pusher), flag(start or pause), process
		self._register = {}
		for job in job_list:
			self._register[job.job] = {
				'class': job,
				'process': None
			}

	def run(self):
		try:
			for job_name in self._register:
				job = self._register[job_name]['class']
				p = multiprocessing.Process(target=job.run, name=job_name)
				self._register[job_name]['process'] = p
				p.start()
			while True:
				self.refresh()
				time.sleep(5)
		except KeyboardInterrupt as e:
			for job_name in self._register:
				p = self._register[job_name]['process']
				p.terminate()
				p.join()
			print(e)

	def refresh(self):
		# print(self._register)
		task_config = self.config.CONFIG['GLOBAL']['JOB']
		new_job_list = {}
		for job_name in task_config:
			if 0 == int(task_config[job_name].get("DO_MR2P", 0)) or not "MR2P_PROCESSOR" in task_config[job_name]:
				continue
			new_job_list[job_name] = task_config[job_name]
		for job_name in new_job_list:
			if not job_name in self._register:
				self.add_job(job_name, new_job_list[job_name])
		for job_name in list(self._register):
			if not job_name in new_job_list:
				self.del_job(job_name)
			else:
				p = self._register[job_name]['process']
				if not p:
					print('start job[%s]...'%job_name)
					new_p = multiprocessing.Process(target=self._register[job_name]['class'].run, name=job_name)
					self._register[job_name]['process'] = new_p
					new_p.start()
				elif not p.is_alive():
					p.join()
					print('restart job[%s]...'%job_name)
					new_p = multiprocessing.Process(target=self._register[job_name]['class'].run, name=job_name)
					self._register[job_name]['process'] = new_p
					new_p.start()

	def add_job(self, name, job):
		self._register[name] = {
			'class': eval(job['MR2P_PROCESSOR'] + "(name)"),
			'process': None
		}

	def del_job(self, name):
		if name in self._register:
			p = self._register[name]['process']
			if not p:
				pass
			elif p.is_alive():
				p.terminate()
				p.join()
			elif not p.is_alive():
				p.join()
			print('delete job[%s]...'%name)
			del self._register[name]


