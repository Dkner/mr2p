import sys
sys.path.append("..")
import getopt
from core.configuration import Configuration
from plugins.company2data import *

if __name__ == '__main__':
    optlist, args = getopt.getopt(sys.argv[1:], 'e:j:', ['env=', 'job='])
    opt_dict = dict(optlist)
    try:
        config = Configuration()
        config.import_global_config(opt_dict['--env'])
        script = Kr2data(opt_dict['--job'])
        # script.run()

        from cProfile import Profile
        from pstats import Stats

        profiler = Profile()
        profiler.runcall(script.run)
        stats = Stats(profiler)
        stats.strip_dirs()
        stats.sort_stats('cumulative')
        stats.print_stats()
    except Exception as e:
        print(e)