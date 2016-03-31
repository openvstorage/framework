# Copyright 2016 iNuron NV
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

import inspect
import logging
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
    propagate_cache = {}
    targets = {'lib': 'lib',
               'api': 'api',
               'extensions': 'extensions',
               'dal': 'dal',
               'celery': 'celery',
               'arakoon': 'arakoon',
               'support': 'support',
               'log': 'audit_trails',
               'storagerouterclient': 'storagerouterclient'}

    def __init__(self, source, name=None, propagate=True):
        """
        Initializes the logger
        """
        parent_invoker = inspect.stack()[1]
        if not __file__.startswith(parent_invoker[1]) or parent_invoker[3] != 'get':
            raise RuntimeError('Cannot invoke instance from outside this class. Please use LogHandler.get(source, name=None) instead')

        if name is None:
            name = 'logger'

        log_filename = LogHandler.load_path(source)

        formatter = logging.Formatter('%(asctime)s - [%(process)s] - [%(levelname)s] - [{0}] - [%(name)s] - %(message)s'.format(source))
        handler = logging.FileHandler(log_filename)
        handler.setFormatter(formatter)

        self.logger = logging.getLogger(name)
        self.logger.propagate = propagate
        self.logger.setLevel(getattr(logging, 'DEBUG'))
        self.logger.addHandler(handler)
        self._key = '{0}_{1}'.format(source, name)

    @staticmethod
    def load_path(source):
        log_filename = '/var/log/ovs/{0}.log'.format(
            LogHandler.targets[source] if source in LogHandler.targets else 'generic'
        )
        if not os.path.exists(log_filename):
            open(log_filename, 'a').close()
            os.chmod(log_filename, 0o666)
        return log_filename

    @staticmethod
    def get(source, name=None, propagate=True):
        key = '{0}_{1}'.format(source, name)
        if key not in LogHandler.cache:
            logger = LogHandler(source, name, propagate)
            LogHandler.cache[key] = logger
        if key not in LogHandler.propagate_cache:
            LogHandler.propagate_cache[key] = propagate
        return LogHandler.cache[key]

    def _fix_propagate(self):
        """
        Obey propagate flag as initially called
        - celery will overwrite it to catch the logging
        """
        propagate = LogHandler.propagate_cache.get(self._key, None)
        if propagate is not None:
            self.logger.propagate = propagate

    @_ignore_formatting_errors()
    def info(self, msg, *args, **kwargs):
        """ Info """
        self._fix_propagate()
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.info(msg, *args, **kwargs)

    @_ignore_formatting_errors()
    def error(self, msg, *args, **kwargs):
        """ Error """
        self._fix_propagate()
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.error(msg, *args, **kwargs)

    @_ignore_formatting_errors()
    def debug(self, msg, *args, **kwargs):
        """ Debug """
        self._fix_propagate()
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.debug(msg, *args, **kwargs)

    @_ignore_formatting_errors()
    def warning(self, msg, *args, **kwargs):
        """ Warning """
        self._fix_propagate()
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.warning(msg, *args, **kwargs)

    @_ignore_formatting_errors()
    def log(self, msg, *args, **kwargs):
        """ Log """
        self._fix_propagate()
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.log(msg, *args, **kwargs)

    @_ignore_formatting_errors()
    def critical(self, msg, *args, **kwargs):
        """ Critical """
        self._fix_propagate()
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.critical(msg, *args, **kwargs)

    @_ignore_formatting_errors()
    def exception(self, msg, *args, **kwargs):
        """ Exception """
        self._fix_propagate()
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        return self.logger.exception(msg, *args, **kwargs)
