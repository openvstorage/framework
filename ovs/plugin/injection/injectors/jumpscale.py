# license see http://www.openvstorage.com/licenses/opensource/
"""
JumpScale Injector module
"""
from JumpScale import j


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
        return provider

    @staticmethod
    def inject_tools(provider):
        """ Injects the Tools module """
        provider.inifile = j.tools.inifile
        return provider

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
        def add_service(package, name, command, stop_command):
            voldrv_package = j.packages.find(domain=package[0], name=package[1])[0]
            j.tools.startupmanager.addProcess(
                name=name, cmd=command, args='', env={}, numprocesses=1, priority=21,
                shell=False, workingdir='', jpackage=voldrv_package,
                domain=voldrv_package.domain, ports=[], stopcmd=stop_command
            )

        provider.add_service = staticmethod(add_service)
        return provider

    @staticmethod
    def inject_console(provider):
        """ Injects the Console module """
        _ = provider
        return j.console

    @staticmethod
    def inject_logger(provider):
        """ Injects the Logger module """
        provider.log = j.logger.log
        return provider

    @staticmethod
    def inject_process(provider):
        """ Injects the Process module """
        _ = provider
        return j.system.process
