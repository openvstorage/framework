// Copyright (C) 2018 iNuron NV
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
/*global define */
/**
 * Service to help with subscription
 */
define([
    'jquery', 'durandal/events',
    'viewmodels/containers/shared/base_container'
], function ($, Events,
             BaseContainer) {

    /**
     * Service to hold event subscriptions. Used to track and dispose of subscription to clean them up when a viewmodel was removed
     * This is wrapper around the Event object of Durandal to store all events
     * @constructor
     */
    // Constants
    var baseContext = "genericSubscriber";

    function SubscriberService() {
        var self = this;
        BaseContainer.call(self);
        Events.call(self);

        // Variables
        self.subscriptions = {};
    }
    var functions = {
        /**
         * Creates a subscription or registers a callback for the specified event which is registered under a context.
         * @method on
         * @param {string} events One or more events, separated by white space.
         * @param viewModelContext: (Optional) The viewModelContext to work in. Can be set to easily dispose of a series of subscriptions
         * @param {function} [callback] The callback function to invoke when the event is triggered. If `callback` is not provided, a subscription instance is returned.
         * @param {object} [context] An object to use as `this` when invoking the `callback`.
         * @return {Subscription|Events} A subscription is returned if no callback is supplied, otherwise the events object is returned for chaining.
         */
        onEvents: function(events, viewModelContext, callback, context) {
            var self = this;
            if (typeof viewModelContext === "undefined") { viewModelContext = baseContext }
            if (!(viewModelContext in self.subscriptions)) {
                self.subscriptions[viewModelContext] = []
            }
            var insertIndex = self.subscriptions[viewModelContext].push( self.on(events, callback, context));
            return self.subscriptions[viewModelContext][insertIndex - 1]
        },
        /**
         * Dispose all subscriptions for a given context
         * @param viewModelContext: Context to dispose everything for
         */
        dispose: function(viewModelContext){
            var self = this;
            if (typeof viewModelContext === "undefined") {
                throw new Error('A viewModelContext needs to be provided')
            }
            var subscriptions = self.subscriptions[viewModelContext] || [];
            while (subscriptions.length) {
                var subscription = subscriptions.shift();
                subscription.off()
            }
        },
        /**
         * Disposes all subscriptions. Note: this is a singleton service so calling this might imply that items are disposed from other viewModelContexts
         */
        disposeAll: function() {
            var self = this;
            $.each(self.subscriptions, function(viewModelContext, subscriptions) {
                self.dispose(viewModelContext);
            })
        }
        // @Todo delete the reference when .off was called so the subscription/Event can be garbage collected (callbacks would be empty though)
        // To keep track of these items, an extra map should be made where a unique id would be referencing the position of the subscription
        // Only required when this would consume too much resources
    };
    SubscriberService.prototype = $.extend({}, Events.prototype, functions);

    return new SubscriberService();
});