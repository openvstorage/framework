import pylibmc
from arakoon import Arakoon


class KeyValueStores(object):
    @staticmethod
    def persistent(cluster):
        arakoon_config = Arakoon.ArakoonClientConfig(cluster,
                                                     {'cfvsa002': (['172.22.1.4'], 8872)})
        return Arakoon.ArakoonClient(config=arakoon_config)

    @staticmethod
    def volatile():
        return pylibmc.Client(['127.0.0.1'], binary=True)