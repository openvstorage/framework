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
import os
import imp
import inspect
from etcd import EtcdConnectionFailed
from ovs.extensions.db.etcd.configuration import EtcdConfiguration
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
        try:
            data = EtcdConfiguration.get(key) if EtcdConfiguration.exists(key) else {}
        except EtcdConnectionFailed:
            import json  # Most likely 2.6 to 2.7 migration
            data = {}
            filename = '/opt/OpenvStorage/config/ovs.json'
            if os.path.exists(filename):
                with open(filename) as config_file:
                    data = json.load(config_file).get('core', {}).get('versions', {})
        migrators = []
        path = '/'.join([os.path.dirname(__file__), 'migration'])
        for filename in os.listdir(path):
            if os.path.isfile('/'.join([path, filename])) and filename.endswith('.py'):
                name = filename.replace('.py', '')
                module = imp.load_source(name, '/'.join([path, filename]))
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
