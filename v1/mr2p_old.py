import os
import sys
import getopt
from core.scheduler import scheduler
from core.configuration import Configuration

for filename in os.listdir(os.path.split(os.path.realpath(__file__))[0] + "/plugins"):
	if filename[-3:] == ".py" and filename[0:-3] != "__init__":
		exec("from plugins import " + filename[0:-3])

def main(env):
	config = Configuration()
	config.import_global_config(env)
	task_config = config.CONFIG['GLOBAL']['JOB']
	task_list = []
	if not isinstance(task_config, dict):
		return
	else:
		for i in task_config:
			# add task when key 'DO_MR2P' equals 1
			if not "DO_MR2P" in task_config or not "MR2P_PROCESSOR" in task_config[i]:
				continue
			elif '0' == task_config[i]['DO_MR2P']:
				continue
			new_task = eval(task_config[i]['MR2P_PROCESSOR']+"(i)")
			processor_num = int(task_config[i].get('PROCESSOR_NUM', 1))
			for i in range(processor_num):
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
