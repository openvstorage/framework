# license see http://www.openvstorage.com/licenses/opensource/
"""
This module contains tools logic
"""


class Tools(object):
    """
    Tools class
    """

    def __init__(self):
        """
        Tools should be a complete static class
        """
        raise RuntimeError('This class should not be instantiated.')

    inifile = None

from ovs.plugin.injection.injector import Injector
Tools = Injector.inject(Tools)
