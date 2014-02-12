# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Contains the loghandler module
"""
import os
import pwd
import logging
import logstash_formatter


class FileLogHandler(object):
    """
    File based log handler
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

class LogHandler(object):
    """
    ElasticSearch based log handler + Optional File based log handler
    In case elasticsearch is not available it will log to file
    """
    def __init__(self, application_name, log_to_file=True):
        """
        application name will become file name in case ES is not available
        log_to_file: tell logger to always log to file, even if ES is running
        """
        from ovs.plugin.provider.configuration import Configuration
        from ovs.plugin.provider.logger import Logger

        serverip = Configuration.get('grid.master.ip')
        Logger.logTargetsClear()
        ltes = Logger.LogTargetElasticSearch(serverip)
        Logger.logTargetAdd(ltes)
        Logger.set_name(application_name.replace('.log', ''))
        file_logger = FileLogHandler(application_name)
        self.logger = Logger
        self.file_logger = file_logger
        self.log_to_file = log_to_file


    def log(self, level, message, category = 'generic', *args, **kwargs):
        """
        do the actual logging
        """
        if self.log_to_file:
            self.file_logger.logger.log(level, category + " " + message, *args, **kwargs)
        try:
            self.logger.log(message, level, category, *args, **kwargs)
        except Exception as ex:
            self.file_logger.logger.log(5, str(ex))
            self.file_logger.logger.log(level, message, *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        self.log(1, msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.log(2, msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.log(3, msg, *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        self.log(3, msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.log(4, msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        self.log(4, msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.log(5, msg, *args, **kwargs)

