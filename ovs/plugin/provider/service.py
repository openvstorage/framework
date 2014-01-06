# license see http://www.openvstorage.com/licenses/opensource/
"""
This module contains service logic
"""


class Service(object):
    """
    Service class
    """

    def __init__(self):
        """
        Service should be a complete static class
        """
        raise RuntimeError('This class should not be instantiated.')

    add_service = None

from ovs.plugin.injection.injector import Injector
Service = Injector.inject(Service)
