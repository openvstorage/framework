# license see http://www.openvstorage.com/licenses/opensource/
"""
This module contains console logic
"""


class Console(object):
    """
    Console class
    """

    def __init__(self):
        """
        Console should be a complete static class
        """
        raise RuntimeError('This class should not be instantiated.')

    askChoice = None
    askString = None
    askInteger = None

from ovs.plugin.injection.injector import Injector
Console = Injector.inject(Console)
