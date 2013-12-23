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
        """ Injects the Config mofule """
        provider.get = j.application.config.get

    @staticmethod
    def inject_tools(provider):
        """ Injects the Tools mofule """
        provider.inifile = j.tools.inifile

    @staticmethod
    def inject_package(provider):
        """ Injects the Package mofule """
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

        provider.is_running = is_running
        provider.start = start
        provider.stop = stop
        provider.get_status = get_status

    @staticmethod
    def inject_service(provider):
        """ Injects the Service mofule """
        def add_service(package, name, command, stop_command):
            voldrv_package = j.packages.find(domain=package[0], name=package[1])[0]
            j.tools.startupmanager.addProcess(
                name=name, cmd=command, args='', env={}, numprocesses=1, priority=21,
                shell=False, workingdir='', jpackage=voldrv_package,
                domain=voldrv_package.domain, ports=[], stopcmd=stop_command
            )

        provider.add_service = add_service

    @staticmethod
    def inject_console(provider):
        """ Injects the Console mofule """
        provider = j.console

    @staticmethod
    def inject_logger(provider):
        """ Injects the Logger mofule """
        provider.log = j.logger.log

    @staticmethod
    def inject_process(provider):
        """ Injects the Process mofule """
        provider = j.system.process
