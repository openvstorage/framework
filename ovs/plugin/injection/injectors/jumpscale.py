# Copyright 2014 CloudFounders NV
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

"""
JumpScale Injector module
"""
from JumpScale import j
from JumpScale import grid
from subprocess import check_output

class Injector(object):
    """
    Injector class, provides all logic to inject
    """

    def __init__(self):
        """
        This class should be fully static
        """
        raise RuntimeError('This class should not be instantiated.')

    @staticmethod
    def inject_configuration(provider):
        """ Injects the Config module """
        provider.get = j.application.config.get
        provider.getInt = j.application.config.getInt
        return provider

    @staticmethod
    def inject_remote(provider):
        """ Injects the remote module """
        import JumpScale.baselib.remote
        return j.remote

    @staticmethod
    def inject_package(provider):
        """ Injects the Package module """
        def is_running(namespace, name):
            package = j.packages.find(domain=namespace, name=name)[0]
            return j.tools.startupmanager.getStatus4JPackage(package)

        def start(namespace, name):
            package = j.packages.find(domain=namespace, name=name)[0]
            j.tools.startupmanager.startJPackage(package)

        def stop(namespace, name):
            package = j.packages.find(domain=namespace, name=name)[0]
            j.tools.startupmanager.stopJPackage(package)

        def get_status(namespace, name):
            core_package = j.packages.find(domain=namespace, name=name)[0]
            return j.tools.startupmanager.getStatus4JPackage(core_package)

        provider.is_running = staticmethod(is_running)
        provider.start = staticmethod(start)
        provider.stop = staticmethod(stop)
        provider.get_status = staticmethod(get_status)
        return provider

    @staticmethod
    def inject_service(provider):
        """ Injects the Service module """
        def add_service(package, name, command, stop_command, params=None):
            _ = params
            package = j.packages.find(domain=package[0], name=package[1])[0]
            j.tools.startupmanager.addProcess(
                name=name, cmd=command, args='', env={}, numprocesses=1, priority=21,
                shell=False, workingdir='', jpackage=package,
                domain=package.domain, ports=[], stopcmd=stop_command
            )

        def get_service_status(process_name):
            for processDef in j.tools.startupmanager.getProcessDefs():
                if process_name == processDef.name:
                    return processDef.isRunning()
            return None

        def remove_service(domain, name):
            j.tools.startupmanager.removeProcess(domain=domain, name=name)

        def disable_service(name):
            check_output('jsprocess -n {0} disable'.format(name))

        def enable_service(name):
            check_output('jsprocess -n {0} enable'.format(name))

        def start_service(name):
            check_output('jsprocess -n {0} start'.format(name))

        def stop_service(name):
            check_output('jsprocess -n {0} stop'.format(name))

        def restart_service(name):
            check_output('jsprocess -n {0} restart'.format(name))

        def service_exists(name):
            return name in check_output('jsprocess list | grep {0} || true'.format(name))

        provider.add_service = staticmethod(add_service)
        provider.remove_service = staticmethod(remove_service)
        provider.get_status = staticmethod(get_service_status)
        provider.disable_service = staticmethod(disable_service)
        provider.enable_service = staticmethod(enable_service)
        provider.start_service = staticmethod(start_service)
        provider.stop_service = staticmethod(stop_service)
        provider.restart_service = staticmethod(restart_service)
        provider.service_exists = staticmethod(service_exists)
        return provider

    @staticmethod
    def inject_process(provider):
        """ Injects the Process module """
        _ = provider
        return j.system.process

    @staticmethod
    def inject_net(provider):
        """ Injects the Net module """
        _ = provider
        return j.system.net

    @staticmethod
    def inject_osis(provider):
        """ Injects the osis module """
        _ = provider
        return j.core.osis
