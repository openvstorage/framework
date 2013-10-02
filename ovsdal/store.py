import memcache
from arakoon import Arakoon


class KeyValueStores(object):
    @staticmethod
    def persistent(cluster):
        arakoon_config = Arakoon.ArakoonClientConfig(cluster,
                                                     {'cfvsa002': (['172.22.1.4'], 8872)})
        return Arakoon.ArakoonClient(config=arakoon_config)

    @staticmethod
    def volatile():
        return memcache.Client(['10.100.138.253:11211'])