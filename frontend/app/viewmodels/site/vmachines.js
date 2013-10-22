define([
    'durandal/app',
    'ovs/shared', 'knockout', 'ovs/generic', 'ovs/authentication', 'ovs/refresher',
    '../containers/vmachine'
], function (app, shared, ko, generic, authentication, Refresher, VMachine) {
    "use strict";
    return function () {
        var self = this;

        // System
        self.shared = shared;
        self.refresher = new Refresher();

        // Data
        self.displayname = 'vMachines';
        self.description = 'This page contains a first overview of the vmachines and their vdisks in our model';

        self.vmachine_headers = [
            { key: 'name',    value: 'Name',   width: 300 },
            { key: undefined, value: 'Disks',  width: undefined },
            { key: undefined, value: '&nbsp;', width: 35 }
        ];
        self.vmachines = ko.observableArray([]);
        self.vmachine_guids =  [];

        // Variables
        self.load_vmachines_handle = undefined;

        // Functions
        self.load = function() {
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
                    var i, item, guids = [];
                    for (i = 0; i < data.length; i += 1) {
                        guids.push(data[i].guid);
                    }
                    for (i = 0; i < guids.length; i += 1) {
                        if ($.inArray(guids[i], self.vmachine_guids) === -1) {
                            self.vmachine_guids.push(guids[i]);
                            self.vmachines.push(new VMachine(guids[i]));
                        }
                    }
                    for (i = 0; i < self.vmachine_guids.length; i += 1) {
                        if ($.inArray(self.vmachine_guids[i], guids) === -1) {
                            self.vmachine_guids.splice(i, 1);
                            self.vmachines.splice(i, 1);
                        }
                    }
                    deferred.resolve();
                })
                .fail(deferred.reject);
            }).promise();
        };
        self.clone = function(guid) {
            var i, vms = self.vmachines();
            for (i = 0; i < vms.length; i += 1) {
                if (vms[i].guid() === guid) {
                    generic.alert_error('Unsupported', 'Cloning is not yet supported for: ' + vms[i].name());
                }
            }
        };

        // Durandal
        self.canActivate = function() { return authentication.validate(); };
        self.activate = function () {
            self.refresher.init('vmachine', self.load, 5000);
            app.trigger('vmachine.refresher:run');
            app.trigger('vmachine.refresher:start');
        };
        self.deactivate = function () {
            app.trigger('vmachine.refresher:stop');
            self.refresher.destroy();
        };
    };
});