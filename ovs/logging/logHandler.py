# license see http://www.openvstorage.com/licenses/opensource/
"""
Contains the loghandler module
"""
import os
import pwd
import logging
import logstash_formatter


class LogHandler(object):
    """
    Log handler
    """
    def __init__(self, logFile):
        """
        This empties the log targets
        """
        self.logger = logging.getLogger()
        logFilePath = os.path.join(os.sep, 'var', 'log', 'ovs', logFile)
        handler = logging.FileHandler(logFilePath)
        user = pwd.getpwnam('ovs')
        os.chown(logFilePath, user.pw_uid, user.pw_gid)
        handler.setFormatter(logstash_formatter.LogstashFormatter())
        self.logger.addHandler(handler)
        self.logger.setLevel(6)

