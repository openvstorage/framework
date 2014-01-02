# license see http://www.openvstorage.com/licenses/opensource/
"""
Unittest Injector module
"""


class Injector(object):
    """
    Injector class, provides all logic to inject. However, the unittest injector
    only provides functionality required in the unittests
    """

    def __init__(self):
        """
        This class should be fully static
        """
        raise RuntimeError('This class should not be instantiated.')

    @staticmethod
    def inject_configuration(provider):
        """ Injects the Config module """
        def get(key):
            return key
        provider.get = staticmethod(get)
        return provider

    @staticmethod
    def inject_tools(provider):
        """ Injects the Tools module """
        return provider

    @staticmethod
    def inject_package(provider):
        """ Injects the Package module """
        return provider

    @staticmethod
    def inject_service(provider):
        """ Injects the Service module """
        return provider

    @staticmethod
    def inject_console(provider):
        """ Injects the Console module """
        _ = provider
        return provider

    @staticmethod
    def inject_logger(provider):
        """ Injects the Logger module """
        return provider

    @staticmethod
    def inject_process(provider):
        """ Injects the Process module """
        return provider
