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
from ovs.extensions.generic.configuration import Configuration


class Migrator(object):
    def __init__(self):
        pass

    @staticmethod
    def migrate():
        """ Executes all migrations. It keeps track of an internal "migration version" which is always increasing by one """

        data = Configuration.get('ovs.core.versions') if Configuration.exists('ovs.core.versions') else {}
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
            version = method(base_version)
            if version > end_version:
                end_version = version
            data[identifier] = end_version

        Configuration.set('ovs.core.versions', data)
