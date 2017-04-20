import sys
sys.path.append("..")
import getopt
from core.configuration import Configuration
from plugins.pusher import pusher

if __name__ == '__main__':
	optlist, args = getopt.getopt(sys.argv[1:], 'e:', ['env='])
	env = ''
	for k, v in optlist:
		if k == '-e' or k == '--env':
			env = v
	if env != '':
		config = Configuration()
		config.import_global_config(env)
		script = pusher('CNLINFO')
		script.mongo2redis()
	else:
		print('[ERROR] lack env config')