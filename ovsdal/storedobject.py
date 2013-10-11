class StoredObject(object):
    persistent = None
    volatile   = None

    def __init__(self):
        if StoredObject.persistent is None:
            from ovsdal.storage.arakoonstore import ArakoonStore
            StoredObject.persistent = ArakoonStore.load()
        if StoredObject.volatile is None:
            from ovsdal.storage.memcached import MemcacheStore
            StoredObject.volatile = MemcacheStore.load()