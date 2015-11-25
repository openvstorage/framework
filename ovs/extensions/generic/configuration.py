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
Generic module for managing the OVS configuration files
"""

import json


class Configuration(object):
    """
    Configuration class
    """

    FILE = '/opt/OpenvStorage/config/{0}.json'

    def __init__(self):
        """
        Dummy init method
        """
        _ = self

    @staticmethod
    def get(key):
        filename, path = key.split('.', 1)
        with open(Configuration.FILE.format(filename), 'r') as config_file:
            config = json.loads(config_file.read())
            temp_config = config
            for entry in path.split('.'):
                temp_config = temp_config[entry]
            return temp_config

    @staticmethod
    def set(key, value):
        filename, path = key.split('.', 1)
        with open(Configuration.FILE.format(filename), 'r') as config_file:
            config = json.loads(config_file.read())
            temp_config = config
            entries = path.split('.')
            if len(entries) > 1:
                for entry in entries[:-1]:
                    if entry in temp_config:
                        temp_config = temp_config[entry]
                    else:
                        temp_config[entry] = {}
                        temp_config = temp_config[entry]
                temp_config[entries[-1]] = value
        contents = json.dumps(config, indent=4)
        with open(Configuration.FILE.format(filename), 'w') as config_file:
            config_file.write(contents)

    @staticmethod
    def delete(key, remove_root=False):
        filename, path = key.split('.', 1)
        with open(Configuration.FILE.format(filename), 'r') as config_file:
            config = json.loads(config_file.read())
            temp_config = config
            entries = path.split('.')
            if len(entries) > 1:
                for entry in entries[:-1]:
                    if entry in temp_config:
                        temp_config = temp_config[entry]
                    else:
                        temp_config[entry] = {}
                        temp_config = temp_config[entry]
                del temp_config[entries[-1]]
            if len(entries) == 1 and remove_root is True:
                del config[entries[0]]
        contents = json.dumps(config, indent=4)
        with open(Configuration.FILE.format(filename), 'w') as config_file:
            config_file.write(contents)

    @staticmethod
    def exists(key):
        try:
            _ = Configuration.get(key)
            return True
        except KeyError:
            return False
