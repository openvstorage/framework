# license see http://www.openvstorage.com/licenses/opensource/
"""
This module contains configuration logic
"""


class Configuration(object):
    """
    Configuration class
    """

    def __init__(self):
        """
        Configuration should be a complete static class
        """
        raise RuntimeError('This class should not be instantiated.')

    get = None

from ovs.plugin.injection.injector import Injector
Configuration = Injector.inject(Configuration)
