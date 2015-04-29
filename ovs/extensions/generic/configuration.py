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
Configuration module
"""

import os
import json
import tempfile
from ovs.log.logHandler import LogHandler

logger = LogHandler('extensions', 'file mutex')


class Configuration(object):
    """
    Manages the configuration, based on a json file
    """
    PATH = '/opt/OpenvStorage/config/ovs.json'

    class Entry(object):
        """
        Represents a single level in the configuration file
        """
        def __init__(self, config, path):
            self.config = config
            self.path = path

        def __getattr__(self, item):
            if item in ['config', 'path']:
                return object.__getattribute__(self, item)
            base = self.config
            for path_key in self.path:
                base = base[path_key]
            value = base[item]
            if isinstance(value, dict):
                return Configuration.ENTRY_CLASS(self.config, self.path + [item])
            else:
                return value

        def __setattr__(self, key, value):
            if key in ['config', 'path']:
                return object.__setattr__(self, key, value)
            base = self.config
            for path_key in self.path:
                base = base[path_key]
            base[key] = value

    ENTRY_CLASS = Entry

    def __init__(self, client=None):
        """
        Initializes a configuration object over a client or local
        """
        self.client = client
        if self.client is None:
            with open(Configuration.PATH, 'r') as config_file:
                self.config = json.loads(config_file.read())
        else:
            self.config = json.loads(client.file_read(self._filename))

    def save(self):
        """
        Saves the configuration file
        """
        contents = json.dumps(self.config, indent=4)
        if self.client is None:
            with open(Configuration.PATH, 'w') as config_file:
                config_file.write(contents)
        else:
            (temp_handle, temp_filename) = tempfile.mkstemp()
            with open(temp_filename, 'w') as config_file:
                config_file.write(contents)
            self.client.dir_ensure(self._dir, recursive=True)
            self.client.file_upload(self._filename, temp_filename)
            os.remove(temp_filename)

    def __getattr__(self, item):
        return getattr(Configuration.ENTRY_CLASS(self.config, []), item)
