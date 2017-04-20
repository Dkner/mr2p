import sys
sys.path.append("..")
import getopt
from core.configuration import Configuration
from plugins.company2b import Company2b

if __name__ == '__main__':
	optlist, args = getopt.getopt(sys.argv[1:], 'e:', ['env='])
	env = ''
	for k, v in optlist:
		if k == '-e' or k == '--env':
			env = v
	if env != '':
		config = Configuration()
		config.import_global_config(env)
		script = Company2b('CNLINFO')
		script.run()
	else:
		print('[ERROR] lack env config')