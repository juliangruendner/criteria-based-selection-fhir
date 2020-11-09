"""
  Copyright notice
  ================
  
  Copyright (C) 2018
      Julian Gruendner   <juliangruendner@googlemail.com>
      License: GNU, see LICENSE for more details.
  
"""

import os
import threading
import logging
import json
from time import gmtime, localtime, strftime
import base64
import configuration

COLOR_RED = 31
COLOR_GREEN = 32
COLOR_YELLOW = 33
COLOR_BLUE = 34
COLOR_PURPLE = 35

time = strftime("%Y-%m-%d", localtime())

logfile = configuration.LOG_DIR + "/" + time + ".log"
handler = logging.FileHandler(logfile)

def colorize(s, color=COLOR_RED):
    return (chr(0x1B) + "[0;%dm" % color + str(s) + chr(0x1B) + "[0m")

class RefIntLogger:
    def __init__(self, logfile='allLog.log'):

        self.logdir = configuration.LOG_DIR
        self.logfile = logfile
        self.separator = "|"

        self.logger = logging.getLogger('refint_logger')
        self.logger.setLevel(int(configuration.LOG_LEVEL))
           
        self.logger.addHandler(handler)

    def set_level(self, loglevel):
        self.logger.setLevel(loglevel)

    def __out(self, msg, head, color):
        tid = threading.current_thread().ident & 0xffffffff
        tid = "<%.8x>" % tid
        time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        return time + "|" + head + " " + tid + " | " + msg

    def info(self, msg):
        self.logger.info(self.__out(msg, "[*]", COLOR_GREEN))

    def warning(self, msg):
        self.logger.warning(self.__out(msg, "[#]", COLOR_YELLOW))

    def error(self, msg):
        self.logger.error(self.__out(msg, "[!]", COLOR_RED))

    def debug(self, msg):
        self.logger.debug(self.__out(msg, "[D]", COLOR_BLUE))

    def getLogfileName(self):
        time = strftime("%Y-%m-%d", localtime())
        logfile = time + ".log"
        return logfile

    def log_message_line(self, html_message):
        message = ""

        my_message_dict = self.get_log_message_attributes(html_message)

        for value in my_message_dict.values():
            message = message + value + self.separator

        self.write_to_log(message)

    def log_message_as_json(self, html_message):
        my_message_dict = self.get_log_message_attributes(html_message)
        self.logger.debug(json.dumps(my_message_dict))
