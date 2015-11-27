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

import time
from ovs.extensions.services.service import ServiceManager
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.system import System


class PluginManager(object):

    @staticmethod
    def install_plugins():
        """
        (Re)load plugins
        """
        if ServiceManager.has_service('ovs-watcher-framework', SSHClient('127.0.0.1', username='root')):
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
                if ServiceManager.has_service(watcher, clients[sr]):
                    print '- Stopping watcher on {0} ({1})'.format(sr.name, sr.ip)
                    ServiceManager.stop_service(watcher, clients[sr])
            for sr in masters:
                print '- Restarting memcached on {0} ({1})'.format(sr.name, sr.ip)
                ServiceManager.restart_service(memcached, clients[sr])
            for sr in masters + slaves:
                if ServiceManager.has_service(watcher, clients[sr]):
                    print '- Starting watcher on {0} ({1})'.format(sr.name, sr.ip)
                    ServiceManager.start_service(watcher, clients[sr])

            print '- Execute model migrations'
            from ovs.dal.helpers import Migration
            Migration.migrate()

            from ovs.lib.helpers.toolbox import Toolbox
            ip = System.get_my_storagerouter().ip
            functions = Toolbox.fetch_hooks('plugin', 'postinstall')
            if len(functions) > 0:
                print '- Execute post installation scripts'
            for function in functions:
                function(ip=ip)
            print 'Installing plugin into Open vStorage: Completed'
