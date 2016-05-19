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
Redis log handler
"""
import logging


class RedisListHandler(logging.Handler):
    """
    Publish messages to Redis channel using a list
    """

    def __init__(self, queue, client, level=logging.NOTSET):
        """
        Create a new logger for the given channel and Redis client.
        """
        logging.Handler.__init__(self, level)
        self.queue = queue
        self.client = client

    def emit(self, record):
        """
        Publish record to Redis logging list
        """
        self.client.rpush(self.queue, self.format(record))


