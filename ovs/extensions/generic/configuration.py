# Copyright 2015 CloudFounders NV
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
Generic module for managing the OVS configuration files
"""

from ConfigParser import RawConfigParser
from ovs.log.logHandler import LogHandler
logger = LogHandler('extensions', name='configuration')


class Configuration(object):
    """
    Configuration class
    """

    FILE = '/opt/OpenvStorage/config/{0}.cfg'

    def __init__(self):
        """
        Dummy init method
        """
        _ = self

    @staticmethod
    def get(key):
        filename, section, item = key.split('.', 2)
        config = RawConfigParser()
        config.read(Configuration.FILE.format(filename))
        return config.get(section, item)

    @staticmethod
    def set(key, value):
        filename, section, item = key.split('.', 2)
        config = RawConfigParser()
        config.read(Configuration.FILE.format(filename))
        config.set(section, item, value)
        with open(Configuration.FILE.format(filename), 'w') as config_file:
            config.write(config_file)

    @staticmethod
    def get_int(key):
        return int(Configuration.get(key))
