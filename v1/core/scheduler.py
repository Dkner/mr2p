import multiprocessing
import os
import time
import core.fileUtil as fileUtil

PID_PATH = os.path.split(os.path.realpath(__file__))[0] + "/../data/"


class scheduler(object):

	def __init__(self, name, job_list):
		self._pid_path = PID_PATH + name + ".pid"
		self._job_list = job_list
		self._job_dict = {}

	def run(self):
		# check r2m process if running or not
		is_started = fileUtil.is_exist(self._pid_path)
		if is_started:
			print(">>>>>>>JOB IS RUNNING,EXIST<<<<<<<<<")
			return False
		process_list = []
		processId = []
		processId.append(os.getpid())

		for job in self._job_list:
			p = multiprocessing.Process(target=job.run)
			self._job_dict[p.name] = job
			process_list.append(p)

		try:
			for p in process_list:
				p.start()
				processId.append(p.pid)
			# write pid file
			fileUtil.store_pid(processId, self._pid_path)

			# deamon loop
			while True:
				update_pid = False
				for x in xrange(0, len(process_list)):
					process = process_list[x]
					if not process.is_alive():
						job = self._job_dict[process.name]
						p = multiprocessing.Process(target=job.run)
						p.start()
						#bind the job to new process->p
						self._job_dict[p.name] = self._job_dict[process.name]
						del(process_list[x])
						#update process_list
						process_list.append(p)
						del(self._job_dict[process.name])
						update_pid = True
				if update_pid:
					#update processID
					processId = []
					processId.append(os.getpid())
					for p in process_list:
						processId.append(p.pid)
					fileUtil.store_pid(processId, self._pid_path)
				time.sleep(5)
			for p in process_list:
				p.join()
		except KeyboardInterrupt, error:
			print("-----EXIST-----")
			for p in process_list:
				p.terminate()
			os.remove(self._pid_path)
		return True
