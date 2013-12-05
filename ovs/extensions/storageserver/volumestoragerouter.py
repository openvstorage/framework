# license see http://www.openvstorage.com/licenses/opensource/
"""
Wrapper class for the storagerouterclient of the voldrv team
"""

from volumedriver.storagerouter import storagerouterclient
from configobj import ConfigObj


class VolumeStorageRouterClient(object):
    """
    Client to access storagerouterclient
    """
    def __init__(self):
        """
        Initializes the wrapper given a configfile for the RPC communication
        """
        config = ConfigObj('/opt/OpenvStorage/config/volumestoragerouterclient.cfg')
        self._host = config['local']['host']
        self._port = int(config['local']['port'])

    def load(self):
        """
        Loads and returns the client
        """
        return storagerouterclient.StorageRouterClient(self._host, self._port)
