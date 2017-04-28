import sys
sys.path.append("..")
import getopt
from core.configuration import Configuration
from plugins.pusher import pusher

if __name__ == '__main__':
	optlist, args = getopt.getopt(sys.argv[1:], 'e:j:', ['env=','job='])
	opt_dict = dict(optlist)
	try:
		if opt_dict['--env']:
			config = Configuration()
			config.import_global_config(opt_dict['--env'])
			script = pusher(opt_dict['--job'])
			script.mongo2redis()
		else:
			print('[ERROR] lack env config')
	except Exception as e:
		print('[ERROR] lack env config: {}'.format(e))