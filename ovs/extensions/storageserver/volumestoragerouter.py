# license see http://www.openvstorage.com/licenses/opensource/
"""
Wrapper class for the storagerouterclient of the voldrv team
"""

from volumedriver.storagerouter import storagerouterclient
from ConfigParser import ConfigParser


class VolumeStorageRouterClient(object):
    """
    Client to access storagerouterclient
    """
    def __init__(self):
        """
        Initializes the wrapper given a configfile for the RPC communication
        """
        config = ConfigParser()
        config.read('/opt/OpenvStorage/ovs/config/volumestoragerouter.cfg')
        self._host = config.defaults()['host']
        self._port = int(config.defaults()['port'])

    def load(self):
        """
        Loads and returns the client
        """
        return storagerouterclient.StorageRouterClient(xmlrpchost=self._host, xmlrpcport=self._port)
