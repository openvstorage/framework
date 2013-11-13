import time
import json
from exceptions import KeyNotFoundException


class DummyPersistentStore(object):
    _path = '/tmp/dummypersistent.json'

    @staticmethod
    def clean():
        import os
        try:
            os.remove(DummyPersistentStore._path)
        except OSError:
            pass

    def _read(self):
        try:
            f = open(self._path, 'r')
            data = json.loads(f.read())
            f.close()
        except IOError:
            data = {}
        return data

    def get(self, key):
        data = self._read()
        if key in data:
            return self._read()[key]
        else:
            raise KeyNotFoundException(key)

    def prefix(self, key):
        data = self._read()
        return [k for k in data.keys() if k.startswith(key)]

    def set(self, key, value):
        data = self._read()
        data[key] = value
        self._save(data)

    def delete(self, key):
        data = self._read()
        if key in data:
            del data[key]
            self._save(data)
        else:
            raise KeyNotFoundException(key)

    def _save(self, data):
        f = open(self._path, 'w+')
        f.write(json.dumps(data, sort_keys=True, indent=2))
        f.close()


class DummyVolatileStore(object):
    _path = '/tmp/dummyvolatile.json'
    _storage = {}
    _timeout = {}

    @staticmethod
    def clean():
        import os
        try:
            os.remove(DummyVolatileStore._path)
        except OSError:
            pass

    def _read(self):
        try:
            f = open(self._path, 'r')
            data = json.loads(f.read())
            f.close()
        except IOError:
            data = {'t': {}, 's': {}}
        return data

    def get(self, key):
        data = self._read()
        if key in data['t'] and data['t'][key] > time.time():
            value = data['s'].get(key)
            if 'ovs_primarykeys_' in key:
                value = set(value)
            return value
        return None

    def set(self, key, value, timeout=99999999):
        if 'ovs_primarykeys_' in key:
            value = list(value)
        data = self._read()
        data['s'][key] = value
        data['t'][key] = time.time() + timeout
        self._save(data)

    def add(self, key, value, timeout=99999999):
        data = self._read()
        if key not in data['s']:
            self.set(key, value, timeout)
            return True
        else:
            return False

    def delete(self, key):
        data = self._read()
        if key in data['s']:
            del data['s'][key]
            del data['t'][key]
            self._save(data)

    def _save(self, data):
        f = open(self._path, 'w+')
        f.write(json.dumps(data, sort_keys=True, indent=2))
        f.close()
