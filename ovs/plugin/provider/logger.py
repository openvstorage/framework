# license see http://www.openvstorage.com/licenses/opensource/
"""
This module contains logger logic
"""


class Logger(object):
    """
    Logger class
    """

    def __init__(self):
        """
        Logger should be a complete static class
        """
        raise RuntimeError('This class should not be instantiated.')

    log = None

from ovs.plugin.injection.injector import Injector
Logger = Injector.inject(Logger)
