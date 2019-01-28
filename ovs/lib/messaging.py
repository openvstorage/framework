# Copyright (C) 2016 iNuron NV
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
Messaging module
"""
import logging
from functools import wraps
from ovs_extensions.generic.filemutex import file_mutex
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.storage.volatilefactory import VolatileFactory


def synchronized():
    """
    Synchronization decorator.
    """
    def wrap(f):
        """
        Returns a wrapped function
        """
        @wraps(f)
        def new_function(*args, **kw):
            """
            Executes the decorated function in a locked context
            """
            filemutex = file_mutex('messaging')
            try:
                filemutex.acquire(wait=60)
                mutex = volatile_mutex('messaging')
                try:
                    mutex.acquire(wait=60)
                    return f(*args, **kw)
                finally:
                    mutex.release()
            finally:
                filemutex.release()
        return new_function
    return wrap


class MessageController(object):
    """
    Controller class for messaging related code. Messaging is used for communication with frontend
    clients. It covers a long-polling scenario providing a realtime-alike experience.
    """
    TIMEOUT = 300
    _cache = VolatileFactory.get_client()
    _logger = logging.getLogger(__name__)

    class Type(object):
        """
        Message types
        """
        TASK_COMPLETE = 'TASK_COMPLETE'
        EVENT = 'EVENT'
        ALL = [TASK_COMPLETE, EVENT]

    @staticmethod
    def all_subscriptions():
        """
        Returns all subscriptions
        """
        return MessageController._cache.get('msg_subscriptions', [])

    @staticmethod
    def subscriptions(subscriber_id):
        """
        Returns all subscriptions for a given subscriber
        """
        return MessageController._cache.get('msg_subscriptions_{0}'.format(subscriber_id), [])

    @staticmethod
    @synchronized()
    def subscribe(subscriber_id, subscriptions):
        """
        Subscribes a given subscriber to a set of Types
        """
        MessageController._cache.set('msg_subscriptions_{0}'.format(subscriber_id), subscriptions, MessageController.TIMEOUT)
        all_subscriptions = MessageController._cache.get('msg_subscriptions', [])
        for subscription in subscriptions:
            if subscription not in all_subscriptions:
                all_subscriptions.append(subscription)
        MessageController._cache.set('msg_subscriptions', all_subscriptions, MessageController.TIMEOUT)

    @staticmethod
    @synchronized()
    def reset_subscriptions(subscriber_id):
        """
        Re-caches all subscriptions
        """
        try:
            subscriber_key = 'msg_subscriptions_{0}'.format(subscriber_id)
            MessageController._cache.set(subscriber_key, MessageController._cache.get(subscriber_key, []), MessageController.TIMEOUT)
            MessageController._cache.set('msg_subscriptions', MessageController._cache.get('msg_subscriptions', []), MessageController.TIMEOUT)
        except Exception:
            MessageController._logger.exception('Error resetting subscriptions')
            raise

    @staticmethod
    def get_messages(subscriber_id, message_id):
        """
        Gets all messages pending for a given subscriber, from a given message id
        """
        try:
            subscriptions = MessageController._cache.get('msg_subscriptions_{0}'.format(subscriber_id), [])
            all_messages = MessageController._cache.get('msg_messages', [])
            messages = []
            last_message_id = 0
            for message in all_messages:
                if message['id'] > last_message_id:
                    last_message_id = message['id']
                if message['id'] > message_id and message['type'] in subscriptions:
                    messages.append(message)
            return messages, last_message_id
        except Exception:
            MessageController._logger.exception('Error loading messages')
            raise

    @staticmethod
    @synchronized()
    def fire(message_type, body):
        """
        Adds a new message to the messaging queue
        """
        last_message_id = max([m['id'] for m in MessageController._cache.get('msg_messages', [])] + [0])
        message = {'id': last_message_id + 1,
                   'type': message_type,
                   'body': body}
        messages = MessageController._cache.get('msg_messages', [])
        messages.append(message)
        MessageController._cache.set('msg_messages', messages, MessageController.TIMEOUT)

    @staticmethod
    def last_message_id():
        """
        Gets the last messageid
        """
        try:
            return max([m['id'] for m in MessageController._cache.get('msg_messages', [])] + [0])
        except Exception:
            MessageController._logger.exception('Error loading last message id')
            raise
