# license see http://www.openvstorage.com/licenses/opensource/
"""
This module contains package logic
"""


class Package(object):
    """
    Package class
    """

    def __init__(self):
        """
        Package should be a complete static class
        """
        raise RuntimeError('This class should not be instantiated.')

    is_running = None
    start = None
    stop = None
    get_status = None

from ovs.plugin.injection.injector import Injector
Package = Injector.inject(Package)
