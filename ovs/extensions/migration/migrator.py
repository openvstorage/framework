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
    def rename_config_files(logger):
        try:
            logger.info('elleuuuh')  #todo remove me
            from ovs.constants.albanode import ASD_CONFIG, ASD_CONFIG_DIR
            from ovs_extensions.constants.arakoon import ARAKOON_BASE, ARAKOON_CONFIG

            for name in Configuration.list(ARAKOON_BASE):
                whole_path = os.path.join(ARAKOON_BASE, name, 'config')
                print whole_path
                Configuration.rename(whole_path, ARAKOON_CONFIG.format(name))
            for asd in Configuration.list(ASD_CONFIG_DIR):
                Configuration.rename(ASD_CONFIG.format(asd), ASD_CONFIG.format(asd))
        except:

            pass

    @staticmethod
    def migrate(master_ips=None, extra_ips=None, logger=None):  #todo remove me logger
        """
        Executes all migrations. It keeps track of an internal "migration version" which is always increasing by one
        :param master_ips: IP addresses of the MASTER nodes
        :param extra_ips: IP addresses of the EXTRA nodes
        """
        Migrator.rename_config_files(logger)  #todo remove me logger
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
                        migrators.append((member[1].identifier, member[1].migrate, member[1].THIS_VERSION))

        for identifier, method, end_version in migrators:
            start_version = data.get(identifier, 0)
            if end_version > start_version:
                data[identifier] = method(start_version, master_ips, extra_ips)
        Configuration.set(key, data)


