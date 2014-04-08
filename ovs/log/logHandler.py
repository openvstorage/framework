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

import logging
import logging.handlers
import ConfigParser


class LogHandler(object):
    """
    Log handler
    """

    targets = {'ovs.lib': 'lib',
               'ovs.api': 'api',
               'ovs.extensions': 'extensions',
               'ovs.dal': 'dal',
               'ovs.celery': 'celery'}

    def __init__(self, source, name=None):
        """
        Initializes the logger
        """

        filename = '/opt/OpenvStorage/config/main.cfg'
        parser = ConfigParser.RawConfigParser()
        parser.read(filename)

        if name is None:
            name = parser.get('logging', 'default_name')

        log_filename = '{0}/{1}.log'.format(
            parser.get('logging', 'path'),
            LogHandler.targets[source] if source in LogHandler.targets else parser.get('logging', 'default_file')
        )

        formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - [%(name)s] %(message)s')
        max_bytes = parser.getint('logging', 'maxbytes')
        backup_count = parser.getint('logging', 'backupcount')
        handler = logging.handlers.RotatingFileHandler(log_filename, maxBytes=max_bytes, backupCount=backup_count)
        handler.setFormatter(formatter)

        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, parser.get('logging', 'level')))
        self.logger.addHandler(handler)

    def info(self, msg, *args, **kwargs):
        """ Info """
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.info(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        """ Error """
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.error(msg, *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        """ Debug """
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.debug(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        """ Warning """
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.warning(msg, *args, **kwargs)

    def log(self, msg, *args, **kwargs):
        """ Log """
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.log(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        """ Critical """
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.critical(msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        """ Exception """
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.exception(msg, *args, **kwargs)
