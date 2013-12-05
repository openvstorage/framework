# license see http://www.openvstorage.com/licenses/opensource/
"""
Wrapper class for the storagerouterclient of the voldrv team
"""

from volumedriver.storagerouter import storagerouterclient
from JumpScale import j


class VolumeStorageRouterClient(object):
    """
    Client to access storagerouterclient
    """
    def __init__(self):
        """
        Initializes the wrapper given a configfile for the RPC communication
        """
        self._host = j.application.config.get('volumedriver.filesystem.xmlrpc.ip')
        self._port = int(j.application.config.get('volumedriver.filesystem.xmlrpc.port'))

    def load(self):
        """
        Loads and returns the client
        """
        return storagerouterclient.StorageRouterClient(self._host, self._port)
