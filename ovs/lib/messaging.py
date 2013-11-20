"""
Messaging module
"""
from celery.signals import task_postrun
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.generic.volatilemutex import VolatileMutex

_cache = VolatileFactory.get_client()


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
            mutex = VolatileMutex('messaging')
            mutex.acquire()
            try:
                return f(*args, **kw)
            finally:
                mutex.release()
        return new_function
    return wrap


class MessageController(object):
    """
    Controller class for messaging related code. Messaging is used for communication with frontend
    clients. It covers a long-polling scenario providing a realtime-alike experience.
    """
    TIMEOUT = 300

    class Type:
        """
        Message types
        """
        TASK_COMPLETE = 'TASK_COMPLETE'
        ALL = [TASK_COMPLETE]

    @staticmethod
    @synchronized()
    def all_subscriptions():
        """
        Returns all subscriptions
        """
        return _cache.get('msg_subscriptions', [])

    @staticmethod
    @synchronized()
    def subscriptions(subscriber_id):
        """
        Returns all subscriptions for a given subscriber
        """
        return _cache.get('msg_subscriptions_%d' % subscriber_id, [])

    @staticmethod
    @synchronized()
    def subscribe(subscriber_id, subscriptions):
        """
        Subscribes a given subscriber to a set of Types
        """
        _cache.set('msg_subscriptions_%d' % subscriber_id, subscriptions, MessageController.TIMEOUT)
        all_subscriptions = _cache.get('msg_subscriptions', [])
        for subscription in subscriptions:
            if subscription not in all_subscriptions:
                all_subscriptions.append(subscription)
        _cache.set('msg_subscriptions', all_subscriptions, MessageController.TIMEOUT)

    @staticmethod
    @synchronized()
    def get_messages(subscriber_id, message_id):
        """
        Gets all messages pending for a given subscriber, from a given message id
        """
        subscriptions = _cache.get('msg_subscriptions_%d' % subscriber_id, [])
        all_messages = _cache.get('msg_messages', [])
        messages = []
        last_message_id = 0
        for message in all_messages:
            if message['id'] > last_message_id:
                last_message_id = message['id']
            if message['id'] > message_id and message['type'] in subscriptions:
                messages.append(message)

        _cache.set('msg_messages', _cache.get('msg_messages', []), MessageController.TIMEOUT)
        _cache.set('msg_subscriptions_%d' % subscriber_id, subscriptions, MessageController.TIMEOUT)
        _cache.set('msg_subscriptions', _cache.get('msg_subscriptions', []), MessageController.TIMEOUT)
        return messages, last_message_id

    @staticmethod
    @synchronized()
    def fire(message_type, body):
        """
        Adds a new message to the messaging queue
        """
        last_message_id = max([m['id'] for m in _cache.get('msg_messages', [])] + [0])
        message = {'id'  : last_message_id + 1,
                   'type': message_type,
                   'body': body}
        messages = _cache.get('msg_messages', [])
        messages.append(message)
        _cache.set('msg_messages', messages, MessageController.TIMEOUT)

    @staticmethod
    @synchronized()
    def last_message_id():
        """
        Gets the last messageid
        """
        return max([m['id'] for m in _cache.get('msg_messages', [])] + [0])


@task_postrun.connect
def task_postrun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None, **kwds):
    """
    Hook for celery postrun event
    """
    _ = sender, task, args, kwargs, kwds
    MessageController.fire(MessageController.Type.TASK_COMPLETE, task_id)
