# license see http://www.openvstorage.com/licenses/opensource/
"""
Mockups module
"""


class StorageRouterClient():
    """
    Mocks the StorageRouterClient
    """

    def __init__(self):
        """
        Dummy init method
        """
        pass

    @staticmethod
    def empty_statistics():
        """
        Returns a fake empty object
        """
        return type('Statistics', (), {})()

    @staticmethod
    def empty_info():
        """
        Returns a fake empty object
        """
        return type('Info', (), {})()

    @staticmethod
    def info(volume_id):
        """
        Return fake info
        """
        _ = volume_id
        return type('Info', (), {})()

    @staticmethod
    def list_snapshots(volume_id):
        """
        Return fake info
        """
        _ = volume_id
        return []


class VolumeStorageRouterClient():
    """
    Mocks the VolumeStorageRouterClient
    """

    def __init__(self):
        """
        Dummy init method
        """
        pass

    def load(self):
        """
        Returns the mocked StorageRouterClient
        """
        _ = self
        return StorageRouterClient()


class VolumeStorageRouter():
    """
    Mocks the VolumeStorageRouter
    """
    VolumeStorageRouterClient = VolumeStorageRouterClient

    def __init__(self):
        """
        Dummy init method
        """
        pass


class Loader():
    """
    Mocks loader class
    """

    def __init__(self):
        """
        Dummy init method
        """
        pass

    @staticmethod
    def load(module):
        """
        Always returns 'unittest'
        """
        _ = module
        return 'unittest'


class LoaderModule():
    """
    Mocks dependency loader module
    """

    Loader = Loader

    def __init__(self):
        """
        Dummy init method
        """
        pass


class Hypervisor():
    """
    Mocks a hypervisor client
    """

    def __init__(self):
        """
        Dummy init method
        """
        pass

    def get_state(self, vmid):
        """
        Always returns running
        """
        _ = self, vmid
        return 'RUNNING'


class Factory():
    """
    Mocks hypervisor factory
    """

    def __init__(self):
        """
        Dummy init method
        """
        pass

    @staticmethod
    def get(hypervisor):
        """
        Always returns 'unittest'
        """
        _ = hypervisor
        return


class FactoryModule():
    """
    Mocks hypervisor factory
    """

    Factory = Factory

    def __init__(self):
        """
        Dummy init method
        """
        pass
