define([
    'durandal/app', 'plugins/dialog',
    'ovs/shared', 'knockout', 'ovs/generic', 'ovs/authentication', 'ovs/refresher', 'ovs/api',
    '../containers/vmachine', '../wizards/clone/index'
], function (app, dialog, shared, ko, generic, authentication, Refresher, api, VMachine, CloneWizard) {
    "use strict";
    return function () {
        var self = this;

        // System
        self.shared = shared;
        self.refresher = new Refresher();
        self.widgets = [];

        // Data
        self.displayname = 'vMachines';
        self.description = 'This page contains a first overview of the vmachines and their vdisks in our model';

        self.vmachine_headers = [
            { key: 'name',    value: 'Name',   width: 300 },
            { key: undefined, value: 'Disks',  width: undefined },
            { key: undefined, value: '&nbsp;', width: 35 },
            { key: undefined, value: '&nbsp;', width: 35 }
        ];
        self.vmachines = ko.observableArray([]);
        self.vmachine_guids =  [];

        // Variables
        self.load_vmachines_handle = undefined;

        // Functions
        self.load = function() {
            return $.Deferred(function (deferred) {
                generic.xhr_abort(self.load_vmachines_handle);
                self.load_vmachines_handle = api.get('vmachines')
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
                    dialog.show(new CloneWizard({
                        modal: true,
                        machineguid: guid
                    }));
                }
            }
        };
        self.deletevm = function(guid) {
            var i, vms = self.vmachines();
            for (i = 0; i < vms.length; i += 1) {
                (function(vm) {
                    if (vm.guid() === guid) {
                        app.showMessage('Are you sure you want to delete "' + vm.name() + '"?', 'Are you sure?', ['Yes', 'No'])
                            .done(function (answer) {
                                if (answer === 'Yes') {
                                    api.del('vmachines/' + vm.guid())
                                    .then(self.shared.tasks.wait)
                                    .done(function () {
                                        generic.alert_success('Machine ' + vm.name() + ' deleted.');
                                    })
                                    .fail(function (error) {
                                        generic.alert_success('Machine ' + vm.name() + ' could not be deleted: ' + error);
                                    });
                                }
                            });
                    }
                }(vms[i]));
            }
        };

        // Durandal
        self.canActivate = function() { return authentication.validate(); };
        self.activate = function () {
            self.refresher.init(self.load, 5000);
            self.refresher.run();
            self.refresher.start();
        };
        self.deactivate = function () {
            var i;
            for (i = 0; i < self.widgets.length; i += 2) {
                self.widgets[i].deactivate();
            }
            self.refresher.stop();
        };
    };
});