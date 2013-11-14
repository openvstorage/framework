from threading import Lock
from celery.signals import task_postrun
from ovs.extensions.storage.memcachefactory import MemcacheFactory
from ovs.extensions.generic.volatilemutex import VolatileMutex

_cache = MemcacheFactory.load()


def synchronized():
    """
    Synchronization decorator.
    """
    def wrap(f):
        def new_function(*args, **kw):
            mutex = VolatileMutex('messaging')
            mutex.acquire()
            try:
                return f(*args, **kw)
            finally:
                mutex.release()
        return new_function
    return wrap


class MessageController(object):
    TIMEOUT = 300

    class Type:
        TASK_COMPLETE = 'TASK_COMPLETE'
        ALL = [TASK_COMPLETE]

    @staticmethod
    @synchronized()
    def all_subscriptions():
        return _cache.get('msg_subscriptions', [])

    @staticmethod
    @synchronized()
    def subscriptions(subscriber_id):
        return _cache.get('msg_subscriptions_%d' % subscriber_id, [])

    @staticmethod
    @synchronized()
    def subscribe(subscriber_id, subscriptions):
        _cache.set('msg_subscriptions_%d' % subscriber_id, subscriptions, MessageController.TIMEOUT)
        all_subscriptions = _cache.get('msg_subscriptions', [])
        for subscription in subscriptions:
            if subscription not in all_subscriptions:
                all_subscriptions.append(subscription)
        _cache.set('msg_subscriptions', all_subscriptions, MessageController.TIMEOUT)

    @staticmethod
    @synchronized()
    def get_messages(subscriber_id, message_id):
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
        return max([m['id'] for m in _cache.get('msg_messages', [])] + [0])


@task_postrun.connect
def task_postrun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None, **kwds):
    MessageController.fire(MessageController.Type.TASK_COMPLETE, task_id)