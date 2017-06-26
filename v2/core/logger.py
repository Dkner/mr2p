import logging

# create logger
logger_name = "MR2P"
LOG = logging.getLogger(logger_name)
LOG.setLevel(logging.DEBUG)

# create handler
fh = logging.StreamHandler()
fh.setLevel(logging.DEBUG)

# create formatter
fmt = "%(asctime)-15s %(levelname)s %(filename)s %(lineno)d %(process)d %(message)s"
datefmt = "%a %d %b %Y %H:%M:%S"
formatter = logging.Formatter(fmt, datefmt)

# add handler and formatter to logger
fh.setFormatter(formatter)
LOG.addHandler(fh)

# LOG.debug('debug message')
# LOG.info('info message')
# LOG.warn('warn message')
# LOG.error('error message')
# LOG.critical('critical message')