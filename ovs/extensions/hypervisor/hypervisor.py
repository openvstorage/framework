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
    def create_vm_from_template(self, name, source_vm, disks, esxhost=None, wait=True):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def clone_vm(self, vmid, name, disks, esxhost=None, wait=False):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def delete_vm(self, vmid, wait=False):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def get_vm_object(self, vmid):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def get_vm_agnostic_object(self, vmid):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def get_vm_object_by_devicename(self, devicename, ip, mountpoint):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def is_datastore_available(self, ip, mountpoint, esxhost=None):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def set_as_template(self, vmid, disks, esxhost=None, wait=False):
        """
        Abstract method
        """
        pass
