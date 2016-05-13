# Copyright 2016 iNuron NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Messaging module
"""
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.generic.filemutex import file_mutex


def synchronized():
    """
    Synchronization decorator.
    """
    def wrap(f):
        """
        Returns a wrapped function
        """
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
        return MessageController._cache.get('msg_subscriptions_%d' % subscriber_id, [])

    @staticmethod
    @synchronized()
    def subscribe(subscriber_id, subscriptions):
        """
        Subscribes a given subscriber to a set of Types
        """
        MessageController._cache.set('msg_subscriptions_%d' % subscriber_id, subscriptions, MessageController.TIMEOUT)
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
        subscriber_key = 'msg_subscriptions_%d' % subscriber_id
        MessageController._cache.set(subscriber_key, MessageController._cache.get(subscriber_key, []), MessageController.TIMEOUT)
        MessageController._cache.set('msg_subscriptions', MessageController._cache.get('msg_subscriptions', []), MessageController.TIMEOUT)

    @staticmethod
    def get_messages(subscriber_id, message_id):
        """
        Gets all messages pending for a given subscriber, from a given message id
        """
        subscriptions = MessageController._cache.get('msg_subscriptions_%d' % subscriber_id, [])
        all_messages = MessageController._cache.get('msg_messages', [])
        messages = []
        last_message_id = 0
        for message in all_messages:
            if message['id'] > last_message_id:
                last_message_id = message['id']
            if message['id'] > message_id and message['type'] in subscriptions:
                messages.append(message)
        return messages, last_message_id

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
        return max([m['id'] for m in MessageController._cache.get('msg_messages', [])] + [0])
