define(['ovs/shared', 'knockout', 'ovs/generic', 'ovs/authentication', '../containers/vmachine'], function (shared, ko, generic, authentication, VMachine) {
    "use strict";
    return {
        // Shared data
        shared: shared,
        // Data
        displayname: 'vMachines',
        description: 'This page contains a first overview of the vmachines and their vdisks in our model',
        vmachines: ko.observableArray([]),
        vmachine_guids: [],

        // Variables
        load_vmachines_handle: undefined,
        // Functions
        load: function() {
            var self = this;
            return $.Deferred(function (deferred) {
                if (self.load_vmachines_handle !== undefined) {
                    self.load_vmachines_handle.abort();
                }
                self.load_vmachines_handle = $.ajax('/api/internal/vmachines/?timestamp=' + generic.gettimestamp(), {
                    type: 'get',
                    contentType: 'application/json',
                    headers: {
                        'Authorization': authentication.header()
                    }
                })
                .done(function (data) {
                    var i, item;
                    for (i = 0; i < data.length; i += 1) {
                        item = data[i];
                        if ($.inArray(item.guid, self.vmachine_guids) === -1) {
                            self.vmachine_guids.push(item.guid);
                            self.vmachines.push(new VMachine(item));
                        }
                    }
                    deferred.resolve();
                })
                .fail(deferred.reject);
            }).promise();
        },

        // Durandal
        canActivate: function() { return authentication.validate(); },
        activate: function() {
            var self = this;
            self.load();
        }
    };
});