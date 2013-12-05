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

    STATISTICS_KEYS = ['cluster_cache_hits',
                       'backend_write_operations',
                       'backend_data_read',
                       'metadata_store_hits',
                       'data_written',
                       'data_read',
                       'write_time',
                       'metadata_store_misses',
                       'backend_data_written',
                       'sco_cache_misses',
                       'backend_read_operations',
                       'sco_cache_hits',
                       'write_operations',
                       'cluster_cache_misses',
                       'read_operations']

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
