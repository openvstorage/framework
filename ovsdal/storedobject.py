from exceptions import InvalidStoreFactoryException


class StoredObject(object):
    _store_factory = None

    def __init__(self):
        # Load backends
        if StoredObject._store_factory is None:
            raise InvalidStoreFactoryException
        try:
            self._persistent = StoredObject._store_factory.persistent()
            self._volatile = StoredObject._store_factory.volatile()
        except:
            raise InvalidStoreFactoryException

    @classmethod
    def set_storefactory(cls, factory):
        StoredObject._store_factory = factory
