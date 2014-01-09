// license see http://www.openvstorage.com/licenses/opensource/
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