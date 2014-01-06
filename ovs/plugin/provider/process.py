# license see http://www.openvstorage.com/licenses/opensource/
"""
This module contains process logic
"""


class Process(object):
    """
    Process class
    """

    def __init__(self):
        """
        Process should be a complete static class
        """
        raise RuntimeError('This class should not be instantiated.')

    checkProcess = None

from ovs.plugin.injection.injector import Injector
Process = Injector.inject(Process)
