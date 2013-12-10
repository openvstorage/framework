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
    FOC_STATUS = {'ok_standalone': 10,
                  'ok_sync': 10,
                  'catch_up': 20,
                  'degraded': 30}

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
