define([
    'knockout',
    './data', 'ovs/authentication', 'ovs/generic', 'ovs/api'
], function(ko, data, authentication, generic, api) {
    "use strict";
    return function () {
        var self = this;

        self.data = data;
        self.can_continue = ko.observable({value: true, reason: undefined});

        self.finish = function() {
            return $.Deferred(function (deferred) {
                var i, data = {}, vm = self.data.vm(),
                    disks = vm.vdisks();
                data.disks = {};
                data.name = self.data.name();
                for (i = 0; i < disks.length; i += 1) {
                    data.disks[disks[i].guid()] = disks[i].selected_snapshot();
                }
                api.post('vmachines/' + vm.guid() + '/clone', data)
                .done(function (data) {
                    deferred.resolve(data);
                })
                .fail(function (xmlHttpRequest) {
                    // We check whether we actually received an error, and it's not the browser navigating away
                    if (xmlHttpRequest.readyState !== 0 && xmlHttpRequest.status !== 0) {
                        deferred.reject();
                    }
                });
            }).promise();
        };
    };
});