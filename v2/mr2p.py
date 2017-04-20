import os
import sys
import getopt
is_py2 = sys.version[0] == '2'
from core.configuration import Configuration
from core.scheduler import scheduler

for filename in os.listdir(os.path.split(os.path.realpath(__file__))[0] + "/plugins"):
	if filename[-3:] == ".py" and filename[0:-3] != "__init__":
		exec("from plugins import " + filename[0:-3])

def main(env):
	# import global config
	config = Configuration()
	config.import_global_config(env)

	task_config = config.CONFIG['GLOBAL']['JOB']
	task_list = []
	for job_name in task_config:
		# add task when key 'DO_MR2P' equals 1
		if 0 == int(task_config[job_name].get("DO_MR2P", 0)) or not "MR2P_PROCESSOR" in task_config[job_name]:
			continue
		new_task = eval(task_config[job_name]['MR2P_PROCESSOR']+"(job_name)")
		task_list.append(new_task)

	o = scheduler("mr2p", task_list)
	o.run()

if __name__ == '__main__':
	optlist, args = getopt.getopt(sys.argv[1:], 'e:', ['env='])
	env = ''
	for k, v in optlist:
		if k == '-e' or k == '--env':
			env = v
	if env != '':
		main(env)
	else:
		print('[ERROR] lack env config')
