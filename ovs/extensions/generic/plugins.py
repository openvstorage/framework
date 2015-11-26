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
            from ovs.dal.lists.storagerouterlist import StorageRouterList
            clients = []
            try:
                for storagerouter in StorageRouterList.get_storagerouters():
                    clients.append(SSHClient(storagerouter, username='root'))
            except UnableToConnectException:
                raise RuntimeError('Not all StorageRouters are reachable')

            for client in clients:
                for service_name in ['watcher-framework', 'memcached']:
                    ServiceManager.stop_service(service_name, client=client)
                    wait = 30
                    while wait > 0:
                        if ServiceManager.get_service_status(service_name, client=client) is False:
                            break
                        time.sleep(1)
                        wait -= 1
                    if wait == 0:
                        raise RuntimeError('Could not stop service: {0}'.format(service_name))

            for client in clients:
                for service_name in ['memcached', 'watcher-framework']:
                    ServiceManager.start_service(service_name, client=client)
                    wait = 30
                    while wait > 0:
                        if ServiceManager.get_service_status(service_name, client=client) is True:
                            break
                        time.sleep(1)
                        wait -= 1
                    if wait == 0:
                        raise RuntimeError('Could not start service: {0}'.format(service_name))

            from ovs.dal.helpers import Migration
            Migration.migrate()

            from ovs.lib.helpers.toolbox import Toolbox
            ip = System.get_my_storagerouter().ip
            functions = Toolbox.fetch_hooks('plugin', 'postinstall')
            for function in functions:
                function(ip=ip)
