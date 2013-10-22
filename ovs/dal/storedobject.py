class StoredObject(object):
    persistent = None
    volatile   = None

    def __init__(self):
        if StoredObject.persistent is None:
            from ovs.dal.storage.arakoonstore import ArakoonStore
            StoredObject.persistent = ArakoonStore.load()
        if StoredObject.volatile is None:
            from ovs.dal.storage.memcached import MemcacheStore
            StoredObject.volatile = MemcacheStore.load()