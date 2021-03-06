import sys
sys.path.append("..")
import getopt
from core.configuration import Configuration
from core.pusher import Pusher

if __name__ == '__main__':
	optlist, args = getopt.getopt(sys.argv[1:], 'e:j:', ['env=','job=','skip='])
	opt_dict = dict(optlist)
	try:
		config = Configuration()
		config.import_global_config(opt_dict['--env'])
		script = Pusher(opt_dict['--job'])
		script.mongo2redis(opt_dict.get('--skip', 0))
	except Exception as e:
		print(e)