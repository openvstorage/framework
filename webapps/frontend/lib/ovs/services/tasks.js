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
/*global define */
define([
    'jquery',
    'ovs/generic', 'ovs/api',
    'ovs/services/messaging'
], function($,
            generic, api,
            messaging) {
    "use strict";
    function Tasks() {
        var self = this;
        self.hooks       = {};
        self.taskIDQueue = [];

        // On task complete events, the task result should be loaded
        // @todo make this obsolete. The wait function should always resolve and not rely on messaging
        messaging.subscribe('TASK_COMPLETE', function(taskID) {
            if (self.hooks.hasOwnProperty(taskID)) {
                self.load(taskID)
                    .done(self.hooks[taskID].resolve)
                    .fail(self.hooks[taskID].reject);
            } else {
                var now = generic.getTimestamp(), i, newQueue = [];
                self.taskIDQueue.push({ id: taskID, timestamp: now });
                for (i = 0; i < self.taskIDQueue.length; i += 1) {
                    if (self.taskIDQueue[i].timestamp >= now - 10000) {
                        newQueue.push(self.taskIDQueue[i]);
                    }
                }
                self.taskIDQueue = newQueue;
            }
        });

        // Bind all prototype method to this instance. Used for backwards compatibility
        $.each(Tasks.prototype, function(key, value) {
            if (generic.isFunction(value)) {
                self[key] = value.bind(self)
            }
        })
    }
    Tasks.prototype = {
        /**
         * Wait until a task is resolved
         * Currently relies on messaging to resolve the task (see messaging.subscribe) in the constructor
         * @param taskID: ID of the task to wait for
         * @return {Promise<T>}
         */
        wait: function(taskID) {
            for (var i = 0; i < this.taskIDQueue.length; i += 1){
                var task = this.taskIDQueue[i];
                if (task.id === taskID) {
                    return this.load(taskID);
                }
            }
            this.hooks[taskID] = $.Deferred();
            return this.hooks[taskID].promise();
        },
        /**
         * Alternative to wait. Does not rely on messaging but instead longpolls on the task state
         * Might not be the better alternative as server side does not implement any longpolling like the messaging does
         * @param taskID: ID of the task to wait for
         * @return {Promise<T>}
         */
        waitPoll: function(taskID) {
            var self = this;
            return api.get('tasks/' + taskID)
                .then(function(data) {
                    if (!data.ready) {
                        return generic.delay(1000).then(function() {
                            return self.waitPoll(taskID)
                        })
                    }
                    if (data.successful) {
                        return data.result;
                    } else {
                        throw data.result
                    }
                })
        },
        /**
         * Load task results
         * @param taskID: ID of the task
         * @return {Promise<T>}
         */
        load: function(taskID) {
            return api.get('tasks/' + taskID)
                .then(function(data) {
                    if (data.successful) {
                        return data.result;
                    } else {
                        throw data.result
                    }})
        },
        /**
         * Validate all tasks
         * Checks the current hooks and resolves where possible
         */
        validateTasks: function() {
            $.each(this.hooks, function(taskID, deferred) {
                api.get('tasks/' + taskID)
                    .done(function(data) {
                        if (data.ready === true) {
                            if (data.successful === true) {
                                deferred.resolve(data.result);
                            } else {
                                deferred.reject(data.result);
                            }
                        }
                    });
            });
        }
    };

    return new Tasks()
});
