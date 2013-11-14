"""
Dummy persistent module
"""

import json
from ovs.extensions.storage.exceptions import KeyNotFoundException


class DummyPersistentStore(object):
    """
    This is a dummy persistent store that makes use of a local json file
    """
    _path = '/tmp/dummypersistent.json'

    @staticmethod
    def clean():
        """
        Empties the store
        """
        import os

        try:
            os.remove(DummyPersistentStore._path)
        except OSError:
            pass

    def _read(self):
        """
        Reads the local json file
        """
        try:
            f = open(self._path, 'r')
            data = json.loads(f.read())
            f.close()
        except IOError:
            data = {}
        return data

    def get(self, key):
        """
        Retrieves a certain value for a given key
        """
        data = self._read()
        if key in data:
            return self._read()[key]
        else:
            raise KeyNotFoundException(key)

    def prefix(self, key):
        """
        Lists all keys starting with the given prefix
        """
        data = self._read()
        return [k for k in data.keys() if k.startswith(key)]

    def set(self, key, value):
        """
        Sets the value for a key to a given value
        """
        data = self._read()
        data[key] = value
        self._save(data)

    def delete(self, key):
        """
        Deletes a given key from the store
        """
        data = self._read()
        if key in data:
            del data[key]
            self._save(data)
        else:
            raise KeyNotFoundException(key)

    def _save(self, data):
        """
        Saves the local json file
        """
        f = open(self._path, 'w+')
        f.write(json.dumps(data, sort_keys=True, indent=2))
        f.close()
