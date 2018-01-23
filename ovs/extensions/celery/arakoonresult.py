# Copyright (C) 2018 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
Arakoon ResultBackend Module
"""

from celery.backends.base import KeyValueStoreBackend
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs_extensions.storage.exceptions import KeyNotFoundException
from ovs.log.log_handler import LogHandler


class ArakoonResultBackend(KeyValueStoreBackend):
    """
    Class to use Arakoon as a result backend for Celery
    """
    _NAMESPACE_PREFIX = 'ovs_tasks_'
    # @TODO handle expiration
    servers = None
    supports_autoexpire = True
    supports_native_join = True
    implements_incr = True

    _logger = LogHandler.get('celery', 'arakoon_result')

    def __init__(self, app, expires=None, backend=None, options=None, url=None, **kwargs):
        super(ArakoonResultBackend, self).__init__(app, **kwargs)
        if options is None:
            options = {}

        self.url = url
        self.options = dict(self.app.conf.CELERY_CACHE_BACKEND_OPTIONS, **options)

        self.backend = url or backend or self.app.conf.CELERY_CACHE_BACKEND  # Will be 'arakoon'
        self.expires = self.prepare_expires(expires, type=int)
        self._encode_prefixes()  # rencode the keyprefixes

        self._client = PersistentFactory.get_client()

    def get(self, key):
        try:
            return self._client.get(key)
        except KeyNotFoundException:
            return None

    def mget(self, keys):
        return self._client.get_multi(keys)

    def set(self, key, value):
        # raise RuntimeError('REMOVE')
        return self._client.set(key, value)

    def delete(self, key):
        return self._client.delete(key)

    def _apply_chord_incr(self, header, partial_args, group_id, body, **opts):
        self._client.set(self.get_key_for_chord(group_id), 0)
        return super(ArakoonResultBackend, self)._apply_chord_incr(header, partial_args, group_id, body, **opts)

    def incr(self, key):
        """
        Increment the value of the key if the value represents an integer
        :param key: Key of which the value should be incremented
        :return: The incremented number (if the value was incrementable)
        """
        val = self._client.get(key)
        if isinstance(val, int):
            val += 1
        return val

    def __reduce__(self, args=(), kwargs=None):
        if kwargs is None:
            kwargs = {}
        kwargs.update(dict(backend=self.backend, expires=self.expires, options=self.options))
        return super(ArakoonResultBackend, self).__reduce__(args, kwargs)

    def as_uri(self, *args, **kwargs):
        """Return the backend as an URI.
        This properly handles the case of multiple servers.
        """
        return self.backend

    def _get_full_key(self, key):
        """
        Generates the key to store in persistent
        :param key: Given key to store
        :return: Key with the namespace
        """
        return '{0}{1}'.format(self._NAMESPACE_PREFIX, key)

    def get_key_for_task(self, *args, **kwargs):
        """Get the cache key for a task by id."""
        return self._get_full_key(super(ArakoonResultBackend, self).get_key_for_task(*args, **kwargs))

    def get_key_for_group(self, *args, **kwargs):
        """Get the cache key for a group by id."""
        return self._get_full_key(super(ArakoonResultBackend, self).get_key_for_group(*args, **kwargs))

    def get_key_for_chord(self, *args, **kwargs):
        """Get the cache key for the chord waiting on group with given id."""
        return self._get_full_key(super(ArakoonResultBackend, self).get_key_for_chord(*args, **kwargs))
