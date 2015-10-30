# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Contains the loghandler module
"""

import inspect
import logging
import os
from ovs.extensions.generic.configuration import Configuration


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
                msg = str(msg)
                return f(self, msg, *args, **kwargs)
            except TypeError as exception:
                too_many = 'not all arguments converted during string formatting' in str(exception)
                not_enough = 'not enough arguments for format string' in str(exception)
                if too_many or not_enough:
                    msg = msg.replace('%', '%%')
                    msg = msg % args
                    msg = msg.replace('%%', '%')
                    return f(self, msg, *[], **kwargs)
                raise

        new_function.__name__ = f.__name__
        new_function.__module__ = f.__module__
        return new_function
    return wrap


class LogHandler(object):
    """
    Log handler
    """

    cache = {}
    targets = {'lib': 'lib',
               'api': 'api',
               'extensions': 'extensions',
               'dal': 'dal',
               'celery': 'celery',
               'arakoon': 'arakoon',
               'support': 'support',
               'log': 'audit_trails',
               'storagerouterclient': 'storagerouterclient'}

    def __init__(self, source, name=None):
        """
        Initializes the logger
        """
        parent_invoker = inspect.stack()[1]
        if not __file__.startswith(parent_invoker[1]) or parent_invoker[3] != 'get':
            raise RuntimeError('Cannot invoke instance from outside this class. Please use LogHandler.get(source, name=None) instead')

        if name is None:
            name = Configuration.get('ovs.logging.default_name')

        log_filename = LogHandler.load_path(source)

        formatter = logging.Formatter('%(asctime)s - [%(process)s] - [%(levelname)s] - [{0}] - [%(name)s] - %(message)s'.format(source))
        handler = logging.FileHandler(log_filename)
        handler.setFormatter(formatter)

        self.logger = logging.getLogger(name)
        self.logger.propagate = True
        self.logger.setLevel(getattr(logging, Configuration.get('ovs.logging.level')))
        self.logger.addHandler(handler)

    @staticmethod
    def load_path(source):
        log_filename = '{0}/{1}.log'.format(
            Configuration.get('ovs.logging.path'),
            LogHandler.targets[source] if source in LogHandler.targets else Configuration.get('ovs.logging.default_file')
        )
        if not os.path.exists(log_filename):
            open(log_filename, 'a').close()
            os.chmod(log_filename, 0o666)
        return log_filename

    @staticmethod
    def get(source, name=None):
        key = '{0}_{1}'.format(source, name)
        if key not in LogHandler.cache:
            logger = LogHandler(source, name)
            LogHandler.cache[key] = logger
        return LogHandler.cache[key]

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
