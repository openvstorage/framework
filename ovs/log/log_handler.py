# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
Contains the loghandler module
"""

import os
import sys
import time
import socket
import inspect
import logging
import itertools


class OVSFormatter(logging.Formatter):
    """
    Formatter for the logger
    """
    def formatTime(self, record, datefmt=None):
        """
        Overrides the default formatter to include UTC offset
        """
        _ = datefmt
        ct = self.converter(record.created)
        tz = time.altzone if time.daylight and ct.tm_isdst > 0 else time.timezone
        offset = '{0}{1:0>2}{2:0>2}'.format('-' if tz > 0 else '+', abs(tz) // 3600, abs(tz // 60) % 60)
        base_time = time.strftime('%Y-%m-%d %H:%M:%S', ct)
        return '{0} {1:03.0f}00 {2}'.format(base_time, record.msecs, offset)

    def format(self, record):
        """
        Format a record
        :param record: Record to format
        :return: Formatted record
        """
        if 'hostname' not in record.__dict__:
            record.hostname = socket.gethostname()
        if 'sequence' not in record.__dict__:
            record.sequence = LogHandler.counter.next()
        return super(OVSFormatter, self).format(record)


class LogHandler(object):
    """
    Log handler.

    WARNING: This log handler might be highly unreliable if not used correctly. It can log to redis, but if Redis is
    not working as expected, it will result in lost log messages. If you want reliable logging, do not use Redis at all
    or log to files and have a separate process forward them to Redis (so logs can be re-send if Redis is unavailable)
    """

    _logs = {}  # Used by unittests

    cache = {}
    counter = itertools.count()
    propagate_cache = {}
    defaults = {'logging_target': {'type': 'console'},
                'level': 'INFO'}

    def __init__(self, source, name, propagate):
        """
        Initializes the logger
        """
        parent_invoker = inspect.stack()[1]
        if not __file__.startswith(parent_invoker[1]) or parent_invoker[3] != 'get':
            raise RuntimeError('Cannot invoke instance from outside this class. Please use LogHandler.get(source, name=None) instead')

        if name is None:
            name = 'logger'

        formatter = OVSFormatter('%(asctime)s - %(hostname)s - %(process)s/%(thread)d - {0}/%(name)s - %(sequence)s - %(levelname)s - %(message)s'.format(source))

        target_definition = LogHandler.load_target_definition(source, allow_override=True)
        if target_definition['type'] == 'redis':
            from redis import Redis
            from ovs.log.redis_logging import RedisListHandler
            self.handler = RedisListHandler(queue=target_definition['queue'],
                                            client=Redis(host=target_definition['host'],
                                                         port=target_definition['port']))
        elif target_definition['type'] == 'file':
            self.handler = logging.FileHandler(target_definition['filename'])
        else:
            self.handler = logging.StreamHandler(sys.stdout)

        self.unittest_mode = False
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            self.unittest_mode = True
        self.handler.setFormatter(formatter)
        self.logger = logging.getLogger(name)
        self.logger.addHandler(self.handler)
        self.logger.propagate = propagate
        self.logger.setLevel(getattr(logging, LogHandler.get_level()))
        self._key = '{0}_{1}'.format(source, name)

    @staticmethod
    def load_target_definition(source, allow_override=False, forced_target_type=None):
        """
        Load the logger target
        :param source: Source
        :type source: str
        :param allow_override: Allow override
        :type allow_override: bool
        :param forced_target_type: Override target type
        :type forced_target_type: str
        :return: Target definition
        :rtype: dict
        """
        logging_target = LogHandler.defaults['logging_target']
        try:
            from ovs.extensions.generic.configuration import Configuration
            logging_target = Configuration.get('/ovs/framework/logging')
        except:
            pass

        target_type = logging_target.get('type', 'console')
        if allow_override is True and 'OVS_LOGTYPE_OVERRIDE' in os.environ:
            target_type = os.environ['OVS_LOGTYPE_OVERRIDE']
        if allow_override is True and forced_target_type is not None:
            target_type = forced_target_type

        if target_type == 'redis':
            queue = logging_target.get('queue', '/ovs/logging')
            if '{0}' in queue:
                queue = queue.format(source)
            return {'type': 'redis',
                    'queue': '/{0}'.format(queue.lstrip('/')),
                    'host': logging_target.get('host', 'localhost'),
                    'port': logging_target.get('port', 6379)}
        if target_type == 'file':
            return {'type': 'file',
                    'filename': LogHandler.load_path(source)}
        return {'type': 'console'}

    @staticmethod
    def get_sink_path(source, allow_override=False, forced_target_type=None):
        """
        Retrieve the path to sink logs to
        :param source: Source
        :type source: str
        :param allow_override: Allow override
        :type allow_override: bool
        :param forced_target_type: Override target type
        :type forced_target_type: str
        :return: The path to sink to
        :rtype: str
        """
        target_definition = LogHandler.load_target_definition(source, allow_override, forced_target_type)
        if target_definition['type'] == 'redis':
            sink = 'redis://{0}:{1}{2}'.format(target_definition['host'], target_definition['port'], target_definition['queue'])
        elif target_definition['type'] == 'file':
            sink = target_definition['filename']
        else:
            sink = 'console:'
        return sink

    @staticmethod
    def get_level():
        level = LogHandler.defaults['level']
        try:
            from ovs.extensions.generic.configuration import Configuration
            level = Configuration.get('/ovs/framework/logging').get('level', level)
        except:
            pass
        return level.upper()

    @staticmethod
    def load_path(source):
        """
        Load path
        :param source: Source
        :return: Path
        """
        log_path = '/var/log/ovs'
        log_filename = '{0}/{1}.log'.format(log_path, source)
        if not os.path.exists(log_path):
            os.mkdir(log_path, 0777)
        if not os.path.exists(log_filename):
            open(log_filename, 'a').close()
            os.chmod(log_filename, 0o666)
        return log_filename

    @staticmethod
    def get(source, name=None, propagate=False):
        """
        Retrieve a loghandler instance
        """
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

    def _log(self, msg, severity, *args, **kwargs):
        """
        Log pass-through
        """
        if self.unittest_mode is True:
            if self._key not in LogHandler._logs:
                LogHandler._logs[self._key] = {}
            LogHandler._logs[self._key][msg.strip()] = severity

        self._fix_propagate()
        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        extra = kwargs.get('extra', {})
        extra['hostname'] = socket.gethostname()
        extra['sequence'] = LogHandler.counter.next()
        kwargs['extra'] = extra
        try:
            return getattr(self.logger, severity)(msg, *args, **kwargs)
        except:
            pass

    def info(self, msg, *args, **kwargs):
        """ Info """
        return self._log(msg, 'info', *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        """ Error """
        return self._log(msg, 'error', *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        """ Debug """
        return self._log(msg, 'debug', *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        """ Warning """
        return self._log(msg, 'warning', *args, **kwargs)

    def log(self, msg, *args, **kwargs):
        """ Log """
        return self._log(msg, 'log', *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        """ Critical """
        return self._log(msg, 'critical', *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        """ Exception """
        return self._log(msg, 'exception', *args, **kwargs)
