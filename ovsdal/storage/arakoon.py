from arakoon import Arakoon


class ArakoonStore(object):
    @staticmethod
    def load():
        arakoon_config = Arakoon.ArakoonClientConfig('openvstorage',
                                                     {'cfvsa002': (['172.22.1.4'], 8872)})
        return Arakoon.ArakoonClient(config=arakoon_config)