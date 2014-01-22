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


class LogHandler(object):
    """
    Log handler
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

