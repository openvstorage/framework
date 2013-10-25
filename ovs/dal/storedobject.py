class StoredObject(object):
    persistent = None
    volatile   = None

    def __init__(self):
        if StoredObject.persistent is None:
            from ovs.extensions.storage.arakoonfactory import ArakoonFactory
            StoredObject.persistent = ArakoonFactory.load()
        if StoredObject.volatile is None:
            from ovs.extensions.storage.memcachefactory import MemcacheFactory
            StoredObject.volatile = MemcacheFactory.load()