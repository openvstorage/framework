# license see http://www.openvstorage.com/licenses/opensource/
"""
This module contains remote logic
"""


class Remote(object):
    """
    Remote class
    """

    def __init__(self):
        """
        Remote should be a complete static class
        """
        raise RuntimeError('This class should not be instantiated.')

    cuisine = None
    fabric = None

from ovs.plugin.injection.injector import Injector
Remote = Injector.inject(Remote)