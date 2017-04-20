import os
import sys
import getopt
is_py2 = sys.version[0] == '2'
from core.egine import EventDriver
from core.configuration import Configuration

for filename in os.listdir(os.path.split(os.path.realpath(__file__))[0] + "/plugins"):
	if filename[-3:] == ".py" and filename[0:-3] != "__init__":
		exec("from plugins import " + filename[0:-3])

def main(env):
	# event driver loop
	eventdriver = EventDriver(maxtask=20)
	eventdriver.start(True)

	# import global config
	config = Configuration()
	config.import_global_config(env)

	task_config = config.CONFIG['GLOBAL']['JOB']
	for i in task_config:
		# add task when key 'DO_MR2P' equals 1
		if 0 == int(task_config[i].get("DO_MR2P", 0)) or not "MR2P_PROCESSOR" in task_config[i]:
			continue
		new_task = eval(task_config[i]['MR2P_PROCESSOR']+"(i, eventdriver)")
		new_task.run()

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
