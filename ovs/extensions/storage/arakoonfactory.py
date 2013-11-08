from arakoon import Arakoon
import json


class ArakoonFactory(object):
    @staticmethod
    def load():
        return ArakoonStore('openvstorage',
                            {'cfvsa002': (['172.22.1.4'], 8872)})


class ArakoonStore(object):
    def __init__(self, cluster, node_config):
        self._cluster = cluster
        self._node_config = node_config
        self._config = Arakoon.ArakoonClientConfig(self._cluster, self._node_config)
        self._client = Arakoon.ArakoonClient(config=self._config)

    def get(self, key):
        return json.loads(self._client.get(key))

    def set(self, key, value):
        return self._client.set(key, json.dumps(value))

    def prefix(self, prefix):
        return self._client.prefix(prefix)

    def delete(self, key):
        return self._client.delete(key)