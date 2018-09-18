// Copyright (C) 2016 iNuron NV
//
// This file is part of Open vStorage Open Source Edition (OSE),
// as available from
//
//      http://www.openvstorage.org and
//      http://www.openvstorage.com.
//
// This file is free software; you can redistribute it and/or modify it
// under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
// as published by the Free Software Foundation, in version 3 as it comes
// in the LICENSE.txt file of the Open vStorage OSE distribution.
//
// Open vStorage is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY of any kind.
/*global define, window */
define([
    'jquery',
    'ovs/shared', 'ovs/api', 'ovs/generic'
], function($, shared, api, generic) {
    "use strict";
    return function() {
        var self = this;

        self.shared        = shared;
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
            return $.Deferred(function(deferred) {
                self.abort = false;
                self.getLastMessageID()
                    .then(function (messageID) {
                        self.lastMessageID = messageID;
                    })
                    .then(self.sendSubscriptions)
                    .done(function () {
                        self.running = true;
                        self.wait();
                        deferred.resolve();
                    })
                    .fail(function () {
                        deferred.reject();
                        throw "Last message id could not be loaded.";
                    });
            }).promise();
        };
        self.stop = function() {
            self.abort = true;
            generic.xhrAbort(self.requestHandle);
            self.running = false;
        };
        self.sendSubscriptions = function() {
            return api.post('messages/' + self.subscriberID + '/subscribe', { data: Object.keys(self.subscriptions) });
        };
        self.wait = function() {
            generic.xhrAbort(self.requestHandle);
            self.requestHandle = api.get('messages/' + self.subscriberID + '/wait', {
                queryparams: { 'message_id': self.lastMessageID },
                timeout: 1000 * 60 * 1.25,
                log: false
            })
                .done(function(data) {
                    var i, subscriptions = Object.keys(self.subscriptions), resubscribe = false;
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
                        window.setTimeout(function() {
                            self.start().always(shared.tasks.validateTasks);
                        }, 5000);
                    }
                });
        };
    };
});
