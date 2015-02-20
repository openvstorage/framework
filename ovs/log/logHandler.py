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
import pwd
import grp
import os


def _ignore_formatting_errors():
    """
    Decorator to ignore formatting errors during logging
    """
    def wrap(f):
        """
        Wrapper function
        """
        def new_function(self, msg, *args, **kwargs):
            """
            Wrapped function
            """
            try:
                _ = msg % args
                return f(self, msg, *args, **kwargs)
            except TypeError as exception:
                if 'not all arguments converted during string formatting' in str(exception):
                    return f(self, 'String format error, original message: {0}'.format(msg))
                else:
                    raise
        return new_function
    return wrap


class LogHandler(object):
    """
    Log handler
    """

    targets = {'lib': 'lib',
               'api': 'api',
               'extensions': 'extensions',
               'dal': 'dal',
               'celery': 'celery',
               'arakoon': 'arakoon'}

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

        uid = pwd.getpwnam('ovs').pw_uid
        gid = grp.getgrnam('ovs').gr_gid
        if not os.path.exists(log_filename):
            open(log_filename, 'a').close()
            os.chmod(log_filename, 0o666)


        formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - [{0}] - [%(name)s] - %(message)s'.format(source))
        handler = logging.FileHandler(log_filename)
        handler.setFormatter(formatter)

        self.logger = logging.getLogger(name)
        self.logger.propagate = True
        self.logger.setLevel(getattr(logging, parser.get('logging', 'level')))
        self.logger.addHandler(handler)

    @_ignore_formatting_errors()
    def info(self, msg, *args, **kwargs):
        """ Info """
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.info(msg, *args, **kwargs)

    @_ignore_formatting_errors()
    def error(self, msg, *args, **kwargs):
        """ Error """
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.error(msg, *args, **kwargs)

    @_ignore_formatting_errors()
    def debug(self, msg, *args, **kwargs):
        """ Debug """
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.debug(msg, *args, **kwargs)

    @_ignore_formatting_errors()
    def warning(self, msg, *args, **kwargs):
        """ Warning """
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.warning(msg, *args, **kwargs)

    @_ignore_formatting_errors()
    def log(self, msg, *args, **kwargs):
        """ Log """
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.log(msg, *args, **kwargs)

    @_ignore_formatting_errors()
    def critical(self, msg, *args, **kwargs):
        """ Critical """
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.critical(msg, *args, **kwargs)

    @_ignore_formatting_errors()
    def exception(self, msg, *args, **kwargs):
        """ Exception """
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.exception(msg, *args, **kwargs)
