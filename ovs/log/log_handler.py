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
Contains the LogHandler module
"""

import inspect
from ovs.extensions.generic.configuration import Configuration, NotFoundException
from ovs_extensions.log.log_handler import LogHandler as _LogHandler


class LogHandler(_LogHandler):
    """
    Log handler.

    WARNING: This log handler might be highly unreliable if not used correctly. It can log to redis, but if Redis is
    not working as expected, it will result in lost log messages. If you want reliable logging, do not use Redis at all
    or log to files and have a separate process forward them to Redis (so logs can be re-send if Redis is unavailable)
    """

    LOG_PATH = '/var/log/ovs'

    def __init__(self, source, name, propagate, target_type):
        """
        Dummy init method
        """
        _ = self, source, name, propagate, target_type
        parent_invoker = inspect.stack()[1]
        if not __file__.startswith(parent_invoker[1]) or parent_invoker[3] != 'get':
            raise RuntimeError('Cannot invoke instance from outside this class. Please use LogHandler.get(source, name=None) instead')

    @classmethod
    def get_logging_info(cls):
        """
        Retrieve logging information from the Configuration management
        :return: Dictionary containing logging information
        :rtype: dict
        """
        try:
            return Configuration.get('/ovs/framework/logging')
        except NotFoundException:
            return {}
