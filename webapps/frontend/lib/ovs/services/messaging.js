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
    'ovs/api', 'ovs/generic'
], function($,
            api, generic) {
    "use strict";

    /**
     * Establish communication with the server
     * Uses longpolling to retrieve message from the server
     * @constructor
     */
    function Messaging() {
        var self = this;

        self.subscriberID  = Math.random().toString().substr(3, 10);
        self.lastMessageID = 0;
        self.requestHandle = undefined;
        self.abort         = false;
        self.subscriptions = {};
        self.running       = false;
    }
    Messaging.prototype = {
        /**
         * Retrieve all the subscriptions
         * @param type: Type of subscription
         * @return {*}
         */
        getSubscriptions: function(type) {
            var callbacks;
            var subscription = this.subscriptions[type];
            if (!subscription) {
                callbacks = $.Callbacks();
                subscription = {
                    publish    : callbacks.fire,
                    subscribe  : callbacks.add,
                    unsubscribe: callbacks.remove
                };
                if (type) {
                    this.subscriptions[type] = subscription;
                }
            }
            return subscription;
        },
        /**
         * Subscribe to a new message type
         * @param type: Type of message
         * @param callback: Callback to execute on receiving the message
         */
        subscribe: function(type, callback) {
            this.getSubscriptions(type).subscribe(callback);
            if (this.running) {
                this.sendSubscriptions();
            }
        },
        /**
         * Unsubscribe
         * @param type: Type of message
         * @param callback: Callback to remove
         */
        unsubscribe: function(type, callback) {
            this.getSubscriptions(type).unsubscribe(callback);
        },
        /**
         * Fire all callbacks
         * @param message: Message to broadcast
         */
        broadcast: function(message) {
            var subscription = this.getSubscriptions(message.type);
            subscription.publish.apply(subscription, [message.body]);
        },
        /**
         * Retrieve the last message sent
         * @return {Promise<String>}
         */
        getLastMessageID: function() {
            return api.get('messages/' + this.subscriberID + '/last');
        },
        /**
         * Start messaging
         */
        start: function() {
            var self = this;
            self.abort = false;
            self.getLastMessageID()
                .then(function (messageID) {
                    self.lastMessageID = messageID;
                })
                .then(self.sendSubscriptions.bind(self))
                .then(function (data) {
                    self.running = true;
                    self.wait();
                    return data
                }, function(error) {
                    throw "Last message id could not be loaded. ({0})".format([error]);
                })
        },
        /**
         * Stop messaging
         */
        stop: function() {
            this.abort = true;
            generic.xhrAbort(this.requestHandle);
            this.running = false;
        },
        sendSubscriptions: function() {
            return api.post('messages/' + this.subscriberID + '/subscribe', { data: Object.keys(this.subscriptions) })
        },
        /**
         * Wait for new messages
         * Resolves when the messaging stops
         */
        wait: function() {
            var self = this;
            generic.xhrAbort(self.requestHandle);
            return self.requestHandle = api.get('messages/' + self.subscriberID + '/wait', {
                queryparams: { 'message_id': self.lastMessageID },
                timeout: 1000 * 60 * 1.25,
                log: false
            })
                .then(function(data) {
                    var subscriptions = Object.keys(self.subscriptions);
                    var resubscribe = false;
                    self.lastMessageID = data.last_message_id;
                    // Broadcast the message
                    $.each(data.messages, function(index, message) {
                        self.broadcast.call(self, message);
                    });
                    $.each(subscriptions, function(index, subscription) {
                        if (data.subscriptions.contains(subscription)) {
                            resubscribe = true;
                            return false;  // Break
                        }
                    });
                    if (resubscribe) {
                        return self.sendSubscriptions.call(self)
                            .then(function(data) {
                                if (!self.abort) {
                                    return self.wait();
                                }
                                return data
                            }, function(error) {
                                // Cycle must continue
                                if (!self.abort) {
                                    return self.wait();
                                }
                                throw error
                            })
                    } else if (!self.abort) {
                        return self.wait();
                    }
                    return data
                }, function(error) {
                    console.warn('Error during longpolling messages ({0}). Restarting in 5 seconds.'.format([error]));
                    // Restart within 5 seconds on failure
                    return $.when().then(function(){
                        if (!self.abort) {
                            return generic.delay(5 * 1000).then(function() {
                                return self.start.call(self)
                            })
                        }
                        throw error
                    })
                })
        }
    };
    return new Messaging()
});
