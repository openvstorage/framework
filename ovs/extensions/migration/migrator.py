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
Migrator module
"""

import os
import imp
import inspect
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.system import System


class Migrator(object):
    """
    Migrator class
    """
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
        data = Configuration.get(key) if Configuration.exists(key) else {}
        migrators = []
        path = '/'.join([os.path.dirname(__file__), 'migration'])
        for filename in os.listdir(path):
            if os.path.isfile('/'.join([path, filename])) and filename.endswith('.py'):
                name = filename.replace('.py', '')
                mod = imp.load_source(name, '/'.join([path, filename]))
                for member in inspect.getmembers(mod, predicate=inspect.isclass):
                    if member[1].__module__ == name and 'object' in [base.__name__ for base in member[1].__bases__]:
                        migrators.append((member[1].identifier, member[1].migrate))

        end_version = 0
        for identifier, method in migrators:
            base_version = data[identifier] if identifier in data else 0
            version = method(base_version, master_ips, extra_ips)
            if version > end_version:
                end_version = version
            data[identifier] = end_version

        Configuration.set(key, data)
