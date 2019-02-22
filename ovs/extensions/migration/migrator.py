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

from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.system import System
from ovs.lib.plugin import PluginController


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
        for member in PluginController.get_migration():
            migrators.append((member.identifier, member.migrate, member.THIS_VERSION))

        for identifier, method, end_version in migrators:
            start_version = data.get(identifier, 0)
            if end_version > start_version:
                data[identifier] = method(start_version, master_ips, extra_ips)
        Configuration.set(key, data)
