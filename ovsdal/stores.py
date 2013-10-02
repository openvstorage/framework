import memcache
from arakoon import Arakoon


class DefaultStoreFactory(object):
    @staticmethod
    def persistent():
        arakoon_config = Arakoon.ArakoonClientConfig('openvstorage',
                                                     {'cfvsa002': (['172.22.1.4'], 8872)})
        return Arakoon.ArakoonClient(config=arakoon_config)

    @staticmethod
    def volatile():
        return memcache.Client(['10.100.138.253:11211'])