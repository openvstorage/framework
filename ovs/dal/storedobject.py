"""
StoredObject module
"""


class StoredObject(object):
    """
    The StoredObject class provides static access to a volatile and persistent store
    When instantiated, and if no stores are yet loaded, it will load the default ones:
    * Persistent: arakoon
    * Volatile: memcache
    """
    persistent = None
    volatile   = None

    def __init__(self):
        if StoredObject.persistent is None:
            from ovs.extensions.storage.arakoonfactory import ArakoonFactory
            StoredObject.persistent = ArakoonFactory.load()
        if StoredObject.volatile is None:
            from ovs.extensions.storage.memcachefactory import MemcacheFactory
            StoredObject.volatile = MemcacheFactory.load()
