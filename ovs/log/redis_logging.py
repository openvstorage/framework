# Copyright 2015 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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


