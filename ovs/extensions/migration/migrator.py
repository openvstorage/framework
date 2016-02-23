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
import os
import imp
import inspect
from ovs.extensions.db.etcd.configuration import EtcdConfiguration
from ovs.extensions.generic.system import System


class Migrator(object):
    def __init__(self):
        pass

    @staticmethod
    def migrate(master_ips=None, extra_ips=None):
        """
        Executes all migrations. It keeps track of an internal "migration version" which is always increasing by one
        :param master_ips: IP addresses of the MASTER nodes
        :param extra_ips: IP addresses of the EXTRA nodes
        """
        machine_id = System.get_my_machine_id()
        key = '/ovs/framework/hosts/{0}/versions'.format(machine_id)
        data = EtcdConfiguration.get(key) if EtcdConfiguration.exists(key) else {}
        migrators = []
        path = os.path.join(os.path.dirname(__file__), 'migration')
        for filename in os.listdir(path):
            if os.path.isfile(os.path.join(path, filename)) and filename.endswith('.py'):
                name = filename.replace('.py', '')
                module = imp.load_source(name, os.path.join(path, filename))
                for member in inspect.getmembers(module):
                    if inspect.isclass(member[1]) and member[1].__module__ == name and 'object' in [base.__name__ for base in member[1].__bases__]:
                        migrators.append((member[1].identifier, member[1].migrate))

        end_version = 0
        for identifier, method in migrators:
            base_version = data[identifier] if identifier in data else 0
            version = method(base_version, master_ips, extra_ips)
            if version > end_version:
                end_version = version
            data[identifier] = end_version

        EtcdConfiguration.set(key, data)
