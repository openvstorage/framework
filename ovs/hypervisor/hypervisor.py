import abc


class Hypervisor(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, ip, username, password):
        self._ip = ip
        self._username = username
        self._password = password
        self._connected = False

    @staticmethod
    def connected(function):
        def new_function(self, *args, **kwargs):
            if not self._connected:
                self._connect()
                self._connected = True
            return function(self, *args, **kwargs)
        return new_function

    @abc.abstractmethod
    def _connect(self):
        pass

    @abc.abstractmethod
    def start(self, vmid):
        pass

    @abc.abstractmethod
    def stop(self, vmid):
        pass
