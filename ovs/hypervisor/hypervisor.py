# license see http://www.openvstorage.com/licenses/opensource/
"""
Module for the abstract Hypervisor object
"""
import abc


class Hypervisor(object):
    """
    Hypervisor abstract class, providing a mandatory set of methods
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, ip, username, password):
        """
        Initializes the class, storing an ip, username and password
        """
        self._ip = ip
        self._username = username
        self._password = password
        self._connected = False

    @staticmethod
    def connected(function):
        """
        Decorator method for making sure the client is connected
        """
        def new_function(self, *args, **kwargs):
            """
            Decorator wrapped function
            """
            if not self._connected:
                self._connect()
                self._connected = True
            return function(self, *args, **kwargs)
        return new_function

    @abc.abstractmethod
    def _connect(self):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def start(self, vmid):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def stop(self, vmid):
        """
        Abstract method
        """
        pass
