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

from types import ModuleType
from subprocess import check_output


class Remote(object):
    def __init__(self, ip, modules, allowed=None, username=None):
        from rpyc.utils.zerodeploy import DeployedServer
        from plumbum import SshMachine

        self.username = username if username is not None else check_output('whoami').strip()
        self.machine = SshMachine(ip, user=self.username)
        self.server = DeployedServer(self.machine)
        self.connection = None
        self.modules = modules
        for module in self.modules:
            if not isinstance(module, ModuleType):
                raise RuntimeError('A non-module was passed in as module: {0}'.format(module))
        self.allowed = allowed if allowed is not None else []
        self.globals = {}

    def __iter__(self):
        replacements = []
        for module in self.modules:
            replacements.append(self.connection.modules[module.__name__])
        return iter(replacements)

    def __enter__(self):
        self.connection = self.server.classic_connect()
        _globals = globals()
        for name in _globals.keys():
            if name not in self.allowed:
                self.globals[name] = _globals[name]
                del _globals[name]
        return self

    def __exit__(self, *args):
        self.connection.close()
        _ = args
        for name in self.globals:
            globals()[name] = self.globals[name]
