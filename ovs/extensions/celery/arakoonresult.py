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
import time
import yaml
from celery.backends.base import KeyValueStoreBackend
from ovs.extensions.generic.logger import Logger
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs_extensions.storage.exceptions import KeyNotFoundException


class ArakoonResultBackend(KeyValueStoreBackend):
    """
    Class to use Arakoon as a result backend for Celery
    Requires PyYAML
    """
    _NAMESPACE_PREFIX = 'ovs_tasks_'
    # Arakoon does not expire of itself. This will enable enable celery beat to add a clean up job every CELERY_TASK_RESULT_EXPIRES seconds (default 1 day)
    supports_autoexpire = False
    supports_native_join = True
    implements_incr = True

    _logger = Logger('celery')

    def __init__(self, app, expires=None, backend=None, options=None, url=None, **kwargs):
        super(ArakoonResultBackend, self).__init__(app, **kwargs)
        if options is None:
            options = {}

        self.url = url
        self.options = dict(self.app.conf.CELERY_CACHE_BACKEND_OPTIONS, **options)

        self.backend = url or backend or self.app.conf.CELERY_RESULT_BACKEND  # Will be 'arakoon'
        self.expires = self.prepare_expires(expires, type=int)
        self._encode_prefixes()  # rencode the keyprefixes

        self._client = PersistentFactory.get_client()

    def get(self, key):
        return self._extract_data(key)

    def mget(self, keys):
        return self._extract_data_multi(keys)

    def set(self, key, value):
        return self._set_data(key, value)

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

    def _extract_data(self, key=None, data=None):
        """
        The entries given in the backend are json by default with expiration information
        :param key: Key to extract data from
        :param data: Data to extract data from
        :return: Extract data or full data
        """
        if key is None and data is None:
            return None  # Could be that the supplied data is None when a mget wants to do an unwrapping
        if data is None:
            try:
                data = self._client.get(key)
            except KeyNotFoundException:
                data = None
        if isinstance(data, dict) and 'data' in data:
            return data['data']
        return data

    def _extract_data_multi(self, keys):
        return (self._extract_data(data=data) for data in self._client.get_multi(keys, must_exist=False))

    def _set_data(self, key, value):
        """
        Wraps the data to save to support expiration
        :param key: Key to store under
        :param value: Value to store
        :return: True if successful, false if not
        """
        # @Todo allow DataObjects to be stored and retrieved
        return self._client.set(key, {'data': value, 'time_set': time.time()})

    def get_key_for_task(self, *args, **kwargs):
        """
        Get the cache key for a task by id.
        """
        return self._get_full_key(super(ArakoonResultBackend, self).get_key_for_task(*args, **kwargs))

    def get_key_for_group(self, *args, **kwargs):
        """
        Get the cache key for a group by id.
        """
        return self._get_full_key(super(ArakoonResultBackend, self).get_key_for_group(*args, **kwargs))

    def get_key_for_chord(self, *args, **kwargs):
        """
        Get the cache key for the chord waiting on group with given id.
        """
        return self._get_full_key(super(ArakoonResultBackend, self).get_key_for_chord(*args, **kwargs))

    def cleanup(self):
        """
        Delete expired metadata. It will not remove results of tasks that are 'STARTED', 'PENDING' or 'RETRY' as the user
        would not be able to retrieve the information of the task if it would be removed
        The clean up task is scheduled by the celery beat. The default expires is 1 day (can be overruled in the settings)
        """
        def build_cleanup():
            transaction = self._client.begin_transaction()  # Faster to batch them all at once than it is to wait for results after every delete
            for key, value in self._client.prefix_entries(self._NAMESPACE_PREFIX):
                if isinstance(value, dict):  # PyrakoonStore wraps it up in a dict and json dumps/loads it
                    if all(k in value for k in ['time_set', 'data']):  # Dealing with a wrapped instance
                        # Will be either JSON or YAML format. JSON will be decoded as dict, YAML as string which needs conversion
                        data = value.get('data')
                        loaded_data = None
                        if isinstance(data, basestring):
                            try:
                                loaded_data = yaml.load(data)
                            except Exception:
                                self._logger.exception('Invalid entry within the ResultBackend')
                        elif isinstance(data, dict):
                            loaded_data = data
                        if isinstance(loaded_data, dict) and 'status' in loaded_data:  # Check for state
                            status = loaded_data['status']
                            # All possible states: PENDING, STARTED, RETRY, FAILURE, SUCCESS
                            if status in ['STARTED', 'RETRY', 'PENDING']:
                                self._logger.debug('Not removing {0} as it has not yet finished'.format(key))
                                continue
                        if time.time() - value['time_set'] > self.expires:
                            self._logger.debug('Removing {0} as it has expired'.format(key))
                            self._client.delete(key, must_exist=False, transaction=transaction)
            self._logger.debug('Applying removal transactions')
            return transaction
        self._client.apply_callback_transaction(build_cleanup, max_retries=20)
