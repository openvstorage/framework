define(['ovs/authentication', 'ovs/api', 'ovs/generic'], function(authentication, api, generic) {
    "use strict";
    return function() {
        var self = this;

        self.subscriber_id = 1234; //Math.random().toString().substr(3, 10);
        self.last_message_id = 0;
        self.request_handle = undefined;
        self.abort = false;
        self.subscriptions = {};
        self.get_subscriptions = function(type) {
            var callbacks,
                subscription = self.subscriptions[type];
            if (subscription === undefined) {
                callbacks = $.Callbacks();
                subscription = {
                    publish    : callbacks.fire,
                    subscribe  : callbacks.add,
                    unsubscribe: callbacks.remove
                };
                if (type !== undefined) {
                    self.subscriptions[type] = subscription;
                }
            }
            return subscription;
        };
        self.subscribe = function(type, callback) {
            self.get_subscriptions(type).subscribe(callback);
            self.send_subscriptions();
        };
        self.unsubscribe = function(type, callback) {
            self.get_subscriptions(type).unsubscribe(callback);
        };
        self.broadcast = function(message) {
            var subscription = self.get_subscriptions(message.type);
            subscription.publish.apply(subscription, [message.body]);
        };
        self.get_last_message_id = function() {
            return api.get('messages/' + self.subscriber_id + '/last');
        };
        self.start = function() {
            self.abort = false;
            self.get_last_message_id()
            .done(function (message_id) {
                self.last_message_id = message_id;
                self.wait();
            })
            .fail(function () {
                throw "Last message id could not be loaded.";
            });
        };
        self.stop = function() {
            self.abort = true;
            generic.xhr_abort(self.request_handle);
        };
        self.send_subscriptions = function() {
            return api.post('messages/' + self.subscriber_id + '/subscribe', generic.keys(self.subscriptions));
        };
        self.wait = function() {
            self.request_handle = api.get('messages/' + self.subscriber_id + '/wait', undefined, {'message_id': self.last_message_id})
            .done(function (data) {
                var i, subscriptions = generic.keys(self.subscriptions), resubscribe = false;
                self.last_message_id = data.last_message_id;
                for (i = 0; i < data.messages.length; i += 1) {
                    self.broadcast(data.messages[i]);
                }
                for (i = 0; i < subscriptions.length; i += 1) {
                    if ($.inArray(subscriptions[i], data.subscriptions) === -1) {
                        resubscribe = true;
                    }
                }
                if (resubscribe) {
                    self.send_subscriptions()
                    .always(function () {
                        if (!self.abort) {
                            self.wait();
                        }
                    });
                } else if (!self.abort) {
                    self.wait();
                }
            })
            .fail(function () {
                if (!self.abort) {
                    window.setTimeout(self.wait, 5000);
                }
            });
        };
    };
});