define([
    'jquery',
    'ovs/shared', 'ovs/generic', 'ovs/api'
], function($, shared, generic, api) {
    "use strict";
    return function() {
        var self = this;

        self.shared = shared;
        self.hooks = {};
        self.wait = function(task_id) {
            self.hooks[task_id] = $.Deferred();
            return self.hooks[task_id].promise();
        };

        self.shared.messaging.subscribe('TASK_COMPLETE', function(task_id) {
            if (self.hooks.hasOwnProperty(task_id)) {
                api.get('tasks/' + task_id)
                    .done(function (data) {
                        if (data.successful === true) {
                            self.hooks[task_id].resolve(data.result);
                        } else {
                            self.hooks[task_id].reject(data.result);
                        }
                    })
                    .fail(function (data) {
                        self.hooks[task_id].reject(data);
                    });
            }
        });
    };
});