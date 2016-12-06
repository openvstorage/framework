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
Remote RPyC wrapper module
"""

import os
from subprocess import check_output
from rpyc.utils.zerodeploy import DeployedServer
from plumbum import SshMachine


class remote(object):
    """
    Remote is a context-manager that allows code within its context to be executed through RPyC
    It is supposed to be used like this:
    with remote([<ip1>, <ip2>], [module1, module2, module3]) as (remote1, remote2):
        remote1.module1.do_something()
        remote2.module3.do_something_else()
    Or like this:
    with remote(<ip1>, [module1, module2, module3]) as remote1:
        remote1.module1.do_something()
    Each module mentioned in the initialization of the remote object will be made available locally (remote1.module1), but will actually be executed remotely on the respective IP (ip1)
    """

    def __init__(self, ip_info, modules, username=None, password=None, strict_host_key_checking=True):
        """
        Initializes the context
        """
        self.ips = []
        self.direct_mode = False
        if isinstance(ip_info, basestring):
            self.ips = [ip_info]
            self.direct_mode = True
        elif isinstance(ip_info, list):
            self.ips = ip_info
        else:
            raise ValueError('IP info needs to be a single IP or a list of IPs')

        if not isinstance(modules, list) and not isinstance(modules, set) and not isinstance(modules, tuple):
            raise ValueError('Modules should be a list, set or tuple')

        self.servers = []
        self.modules = modules
        self._remote_modules = {}
        self._unittest_mode = os.environ.get('RUNNING_UNITTESTS') == 'True'
        if self._unittest_mode is False:
            ssh_opts = []
            if strict_host_key_checking is False:
                ssh_opts.append('-o StrictHostKeyChecking=no')
            self.username = username if username is not None else check_output('whoami').strip()
            self.machines = [SshMachine(ip, user=self.username, password=password, ssh_opts=tuple(ssh_opts)) for ip in self.ips]
            self.servers = [DeployedServer(machine) for machine in self.machines]

    def __iter__(self):
        replacements = []
        for connection in self.connections:
            replacements.append(self._build_remote_module(connection))
        return iter(replacements)

    def __enter__(self):
        if self._unittest_mode is True:
            self.connections = self.ips
        else:
            self.connections = [server.classic_connect() for server in self.servers]
        if self.direct_mode is True:
            return self._build_remote_module(self.connections[0])
        return self

    def __exit__(self, *args):
        _ = args
        for server in self.servers:
            server.close()

    def __getitem__(self, item):
        if item not in self._remote_modules:
            self._remote_modules[item] = self._get_connection(item)
        return self._remote_modules[item]

    def _build_remote_module(self, connection):
        if self._unittest_mode is False:
            connection.modules['sys'].path.append('/opt/OpenvStorage')
        remote_modules = {}
        for module in self.modules:
            if self._unittest_mode is True:
                remote_modules[module.__name__] = module
            elif hasattr(module, '__module__'):
                remote_modules[module.__name__] = getattr(connection.modules[module.__module__], module.__name__)
            else:
                remote_modules[module.__name__] = connection.modules[module.__name__]
        return type('remote', (), remote_modules)

    def _get_connection(self, ip):
        self.connection = self.servers[self.ips.index(ip)].classic_connect()
        return self._build_remote_module(self.connection)
