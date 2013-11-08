/*global define */
define([
    'jquery',
    'ovs/api', 'ovs/generic'
], function($, api, generic) {
    "use strict";
    return function() {
        var self = this;

        self.subscriberID  = Math.random().toString().substr(3, 10);
        self.lastMessageID = 0;
        self.requestHandle = undefined;
        self.abort         = false;
        self.subscriptions = {};
        self.running       = false;

        self.getSubscriptions = function(type) {
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
            self.getSubscriptions(type).subscribe(callback);
            if (self.running) {
                self.sendSubscriptions();
            }
        };
        self.unsubscribe = function(type, callback) {
            self.getSubscriptions(type).unsubscribe(callback);
        };
        self.broadcast = function(message) {
            var subscription = self.getSubscriptions(message.type);
            subscription.publish.apply(subscription, [message.body]);
        };
        self.getLastMessageID = function() {
            return api.get('messages/' + self.subscriberID + '/last');
        };
        self.start = function() {
            self.abort = false;
            self.getLastMessageID()
                .then(function(messageID) {
                    self.lastMessageID = messageID;
                })
                .then(self.sendSubscriptions)
                .done(function() {
                    self.running = true;
                    self.wait();
                })
                .fail(function() {
                    throw "Last message id could not be loaded.";
                });
        };
        self.stop = function() {
            self.abort = true;
            generic.xhrAbort(self.requestHandle);
            self.running = false;
        };
        self.sendSubscriptions = function() {
            return api.post('messages/' + self.subscriberID + '/subscribe', generic.keys(self.subscriptions));
        };
        self.wait = function() {
            self.requestHandle = api.get('messages/' + self.subscriberID + '/wait', undefined, {'message_id': self.lastMessageID})
                .done(function(data) {
                    var i, subscriptions = generic.keys(self.subscriptions), resubscribe = false;
                    self.lastMessageID = data.last_message_id;
                    for (i = 0; i < data.messages.length; i += 1) {
                        self.broadcast(data.messages[i]);
                    }
                    for (i = 0; i < subscriptions.length; i += 1) {
                        if ($.inArray(subscriptions[i], data.subscriptions) === -1) {
                            resubscribe = true;
                        }
                    }
                    if (resubscribe) {
                        self.sendSubscriptions()
                        .always(function() {
                            if (!self.abort) {
                                self.wait();
                            }
                        });
                    } else if (!self.abort) {
                        self.wait();
                    }
                })
                .fail(function() {
                    if (!self.abort) {
                        window.setTimeout(self.wait, 5000);
                    }
                });
        };
    };
});