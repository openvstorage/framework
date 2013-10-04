import memcache


class MemcacheStore(object):
    @staticmethod
    def load():
        return memcache.Client(['10.100.138.253:11211'])
