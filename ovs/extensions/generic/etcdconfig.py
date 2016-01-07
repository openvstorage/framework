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
Generic module for managing configuration in Etcd
"""

import json
import etcd


class EtcdConfiguration(object):
    """
    Configuration class using Etcd.

    Uses a special key format to specify the path within etcd, and specify a path inside the json data
    object that might be stored inside the etcd key.
    key  = <etcd path>[|<json path>]
    etcd path = slash-delimited path
    json path = dot-delimited path
    If no json path is given, the raw etcd data is returned
    """

    def __init__(self):
        """
        Dummy init method
        """
        _ = self

    @staticmethod
    def get(key):
        key_entries = key.split('|')
        client = etcd.Client(port=2379, use_proxies=True)
        data = json.loads(client.read(key_entries[0]).value)
        if len(key_entries) == 1:
            return data
        temp_data = data
        for entry in key_entries[1].split('.'):
            temp_data = temp_data[entry]
        return temp_data

    @staticmethod
    def set(key, value):
        key_entries = key.split('|')
        client = etcd.Client(port=2379, use_proxies=True)
        if len(key_entries) == 1:
            client.write(key_entries[0], json.dumps(value))
            return
        try:
            data = json.loads(client.read(key_entries[0]).value)
        except etcd.EtcdKeyNotFound:
            data = {}
        temp_config = data
        entries = key_entries[1].split('.')
        for entry in entries[:-1]:
            if entry in temp_config:
                temp_config = temp_config[entry]
            else:
                temp_config[entry] = {}
                temp_config = temp_config[entry]
        temp_config[entries[-1]] = value
        client.write(key_entries[0], json.dumps(data))

    @staticmethod
    def delete(key, remove_root=False):
        key_entries = key.split('|')
        client = etcd.Client(port=2379, use_proxies=True)
        if len(key_entries) == 1:
            client.delete(key_entries[0])
            return
        data = json.loads(client.read(key_entries[0]).value)
        temp_config = data
        entries = key_entries[1].split('.')
        if len(entries) > 1:
            for entry in entries[:-1]:
                if entry in temp_config:
                    temp_config = temp_config[entry]
                else:
                    temp_config[entry] = {}
                    temp_config = temp_config[entry]
            del temp_config[entries[-1]]
        if len(entries) == 1 and remove_root is True:
            del data[entries[0]]
        client.write(key_entries[0], json.dumps(data))

    @staticmethod
    def exists(key):
        try:
            _ = EtcdConfiguration.get(key)
            return True
        except (KeyError, etcd.EtcdKeyNotFound):
            return False
