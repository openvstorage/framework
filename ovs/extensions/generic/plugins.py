# Copyright 2015 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ovs.plugin.provider.service import Service
from ovs.extensions.generic.system import System
from ovs.extensions.generic.sshclient import SSHClient


class PluginManager(object):

    @staticmethod
    def load_plugins():
        """
        (Re)load plugins
        """
        if Service.has_service('ovs-watcher-framework'):
            # If the watcher is running, 'ovs setup' was executed and we need to restart everything to load
            # the plugin. In the other case, the plugin will be loaded once 'ovs setup' is executed
            from ovs.dal.lists.storagerouterlist import StorageRouterList
            ips = [storagerouter.ip for storagerouter in StorageRouterList.get_storagerouters()]
            for ip in ips:
                client = SSHClient.load(ip)
                System.exec_remote_python(client, """
import time
from ovs.plugin.provider.service import Service
for s in ['watcher-framework', 'memcached']:
    Service.stop_service(s)
    wait = 30
    while wait > 0:
        if Service.get_service_status(s) is False:
            break
        time.sleep(1)
        wait -= 1
    if wait == 0:
        raise RuntimeError('Could not stop service: {0}'.format(s))
""")
            for ip in ips:
                client = SSHClient.load(ip)
                System.exec_remote_python(client, """
import time
from ovs.plugin.provider.service import Service
for s in ['memcached', 'watcher-framework']:
    Service.start_service(s)
    wait = 30
    while wait > 0:
        if Service.get_service_status(s) is True:
            break
        time.sleep(1)
        wait -= 1
    if wait == 0:
        raise RuntimeError('Could not start service: {0}'.format(s))
""")
            from ovs.dal.helpers import Migration
            Migration.migrate()

            from ovs.extensions.generic.system import System
            from ovs.lib.helpers.toolbox import Toolbox
            ip = System.get_my_storagerouter().ip
            functions = Toolbox.fetch_hooks('plugin', 'postinstall')
            for function in functions:
                function(ip=ip)
