// Copyright 2014 CloudFounders NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define([
    'jquery',
    'ovs/shared', 'ovs/generic', 'ovs/api'
], function($, shared, generic, api) {
    "use strict";
    return function() {
        var self = this;

        self.shared      = shared;
        self.hooks       = {};
        self.taskIDQueue = [];

        self.wait = function(taskID) {
            var i;
            for (i = 0; i < self.taskIDQueue.length; i += 1){
                if (self.taskIDQueue[i].id === taskID) {
                    return self.load(taskID);
                }
            }
            self.hooks[taskID] = $.Deferred();
            return self.hooks[taskID].promise();
        };
        self.load = function(taskID) {
            return $.Deferred(function(deferred) {
                api.get('tasks/' + taskID)
                    .done(function(data) {
                        if (data.successful === true) {
                            deferred.resolve(data.result);
                        } else {
                            deferred.reject(data.result);
                        }
                    })
                    .fail(function(error) {
                        deferred.reject(error);
                    });
            }).promise();
        };
        self.validateTasks = function() {
            $.each(self.hooks, function(taskID, deferred) {
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
        };

        self.shared.messaging.subscribe('TASK_COMPLETE', function(taskID) {
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
    };
});
