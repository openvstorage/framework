from volumedriver.storagerouter import storagerouterclient
from ConfigParser import ConfigParser


class VolumeStorageRouterClient(object):
    def __init__(self):
        config = ConfigParser()
        config.read('/opt/openvStorage/ovs/config/volumestoragerouter.cfg')
        self._host = config.defaults()['host']
        self._port = int(config.defaults()['port'])

    def load(self):
        return storagerouterclient.StorageRouterClient(xmlrpchost=self._host, xmlrpcport=self._port)