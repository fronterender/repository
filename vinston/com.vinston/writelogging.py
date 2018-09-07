import logging
import os
import time
from logging.handlers import TimedRotatingFileHandler
import sys


logger = logging.getLogger("vinston-ETL")
logger.setLevel(logging.DEBUG)
cwd = os.getcwd()
# 当前目录
dirname = cwd+"/logs/"
rollHandler = TimedRotatingFileHandler(dirname+"vinston",encoding="utf-8",when ="D",interval = 1,backupCount=10)
formatter = logging.Formatter("%(asctime)s   [%(name)s]   %(filename)s\t行 %(lineno)s\t%(levelname)s:\t%(message)s")
rollHandler.setFormatter(formatter)
logger.addHandler(rollHandler)
