class StoredObject(object):
    persistent = None
    volatile   = None

    @classmethod
    def set_stores(cls, persistent_store, volatile_store):
        StoredObject.persistent = persistent_store
        StoredObject.volatile   = volatile_store
