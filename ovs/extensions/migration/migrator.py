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
        from ovs.lib.plugin import PluginController
        for member in PluginController.get_migration(): #todo check of nog dict of niet
            migrators.append((member[1].identifier, member[1].migrate, member[1].THIS_VERSION))

        for identifier, method, end_version in migrators:
            start_version = data.get(identifier, 0)
            if end_version > start_version:
                data[identifier] = method(start_version, master_ips, extra_ips)
        Configuration.set(key, data)
