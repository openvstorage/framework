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
"""
Remote RPyC wrapper module
"""

from subprocess import check_output
from rpyc.utils.zerodeploy import DeployedServer
from plumbum import SshMachine


class Remote(object):
    """
    Remote is a context-manager that allows code within its context to be executed through RPyC
    It is supposed to be used like this:
    with Remote([<ip1>, <ip2>], [module1, module2, module3]) as (remote1, remote2):
        remote1.module1.do_something()
        remote2.module3.do_something_else()
    Or like this:
    with Remote(<ip1>, [module1, module2, module3]) as remote1:
        remote1.module1.do_something()
    Each module mentioned in the initialization of the remote object will be made available locally (remote1.module1), but will actually be executed remotely on the respective IP (ip1)
    """

    def __init__(self, ip_info, modules, username=None, password=None, strict_host_key_checking=True):
        """
        Initializes the context
        """
        self.ips = []
        if isinstance(ip_info, basestring):
            self.ips = [ip_info]
        elif isinstance(ip_info, list):
            self.ips = ip_info
        else:
            raise ValueError('IP info needs to be a single IP or a list of IPs')

        if not isinstance(modules, list) and not isinstance(modules, set) and not isinstance(modules, tuple):
            raise ValueError('Modules should be a list, set or tuple')

        self.username = username if username is not None else check_output('whoami').strip()
        ssh_opts = []
        if strict_host_key_checking is False:
            ssh_opts.append('-o StrictHostKeyChecking=no')
        self.machines = [SshMachine(ip, user=self.username, password=password, ssh_opts=tuple(ssh_opts)) for ip in self.ips]
        self.servers = [DeployedServer(machine) for machine in self.machines]
        self.modules = modules

    def __iter__(self):
        replacements = []
        for connection in self.connections:
            replacements.append(self._build_remote_module(connection))
        return iter(replacements)

    def __enter__(self):
        self.connections = [server.classic_connect() for server in self.servers]
        if len(self.connections) == 1:
            return self._build_remote_module(self.connections[0])
        return self

    def __exit__(self, *args):
        _ = args
        for server in self.servers:
            server.close()

    def _build_remote_module(self, connection):
        connection.modules['sys'].path.append('/opt/OpenvStorage')
        remote_modules = {}
        for module in self.modules:
            if hasattr(module, '__module__'):
                remote_modules[module.__name__] = getattr(connection.modules[module.__module__], module.__name__)
            else:
                remote_modules[module.__name__] = connection.modules[module.__name__]
        return type('Remote', (), remote_modules)
