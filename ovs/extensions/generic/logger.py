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
Contains the Logger module
"""
from ovs_extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonException
from ovs.extensions.generic.configuration import Configuration, NotFoundException
from ovs_extensions.log.logger import Logger as _Logger


class Logger(_Logger):
    """
    Logger class

    WARNING: the use of this logger is deprecated in favor of using python default logging
    """
    LOG_PATH = '/var/log/ovs'

    def __init__(self, name, forced_target_type=None):
        """
        Init method
        """
        super(Logger, self).__init__(name, forced_target_type)

    @classmethod
    def get_logging_info(cls):
        """
        Retrieve logging information from the Configuration management
        :return: Dictionary containing logging information
        :rtype: dict
        """
        try:
            return Configuration.get('/ovs/framework/logging')
        except (IOError, NotFoundException, ArakoonException):
            return {}
