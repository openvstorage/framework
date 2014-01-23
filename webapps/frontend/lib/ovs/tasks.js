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

        self.shared = shared;
        self.hooks = {};
        self.wait = function(taskID) {
            self.hooks[taskID] = $.Deferred();
            return self.hooks[taskID].promise();
        };

        self.shared.messaging.subscribe('TASK_COMPLETE', function(taskID) {
            if (self.hooks.hasOwnProperty(taskID)) {
                api.get('tasks/' + taskID)
                    .done(function(data) {
                        if (data.successful === true) {
                            self.hooks[taskID].resolve(data.result);
                        } else {
                            self.hooks[taskID].reject(data.result);
                        }
                    })
                    .fail(function(data) {
                        self.hooks[taskID].reject(data);
                    });
            }
        });
    };
});
