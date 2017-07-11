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
Plugin module
"""

from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.system import System
from ovs.extensions.services.servicefactory import ServiceFactory


class PluginManager(object):
    """
    Plugin Manager class
    """

    @staticmethod
    def install_plugins():
        """
        (Re)load plugins
        """
        manager = ServiceFactory.get_manager()
        if manager.has_service('ovs-watcher-framework', SSHClient('127.0.0.1', username='root')):
            # If the watcher is running, 'ovs setup' was executed and we need to restart everything to load
            # the plugin. In the other case, the plugin will be loaded once 'ovs setup' is executed
            print 'Installing plugin into Open vStorage'
            from ovs.dal.lists.storagerouterlist import StorageRouterList
            clients = {}
            masters = StorageRouterList.get_masters()
            slaves = StorageRouterList.get_slaves()
            try:
                for sr in masters + slaves:
                    clients[sr] = SSHClient(sr, username='root')
            except UnableToConnectException:
                raise RuntimeError('Not all StorageRouters are reachable')
            memcached = 'memcached'
            watcher = 'watcher-framework'
            for sr in masters + slaves:
                if manager.has_service(watcher, clients[sr]):
                    print '- Stopping watcher on {0} ({1})'.format(sr.name, sr.ip)
                    manager.stop_service(watcher, clients[sr])
            for sr in masters:
                print '- Restarting memcached on {0} ({1})'.format(sr.name, sr.ip)
                manager.restart_service(memcached, clients[sr])
            for sr in masters + slaves:
                if manager.has_service(watcher, clients[sr]):
                    print '- Starting watcher on {0} ({1})'.format(sr.name, sr.ip)
                    manager.start_service(watcher, clients[sr])

            print '- Execute model migrations'
            from ovs.dal.helpers import Migration
            Migration.migrate()

            from ovs.lib.helpers.toolbox import Toolbox
            ip = System.get_my_storagerouter().ip
            functions = Toolbox.fetch_hooks('plugin', 'postinstall')
            if len(functions) > 0:
                print '- Execute post installation scripts'
            for fct in functions:
                fct(ip=ip)
            print 'Installing plugin into Open vStorage: Completed'
