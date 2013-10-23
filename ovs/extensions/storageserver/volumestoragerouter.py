from volumedriver.storagerouter import storagerouterclient
from ConfigParser import ConfigParser

class VolumeStorageRouterClient(object):
    def __init__(self):
        Config = ConfigParser()
        Config.read('ovs/config/volumestoragerouter.cfg')
        self._host = Config.defaults()['host']
        self._port = int(Config.defaults()['port'])

    def load(self):
        return storagerouterclient.StorageRouterClient(xmlrpchost=self._host, xmlrpcport=self._port)