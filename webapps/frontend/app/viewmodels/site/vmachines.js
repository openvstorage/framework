/*global define */
define([
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vmachine', '../wizards/clone/index'
], function($, app, dialog, ko, shared, generic, Refresher, api, VMachine, CloneWizard) {
    "use strict";
    return function() {
        var self = this;

        // System
        self.shared    = shared;
        self.guard     = { authenticated: true };
        self.refresher = new Refresher();
        self.widgets   = [];

        // Data
        self.vMachineHeaders = [
            { key: 'name',       value: 'Name',        width: 150,       colspan: undefined },
            { key: undefined,    value: 'vDisks',      width: 60,        colspan: undefined },
            { key: 'storedData', value: 'Stored data', width: 100,       colspan: undefined },
            { key: 'cache',      value: 'Cache',       width: 70,        colspan: undefined },
            { key: 'iops',       value: 'IOPS',        width: 55,        colspan: undefined },
            { key: 'latency',    value: 'Latency',     width: 70,        colspan: undefined },
            { key: 'readSpeed',  value: 'Read speed',  width: 100,       colspan: undefined },
            { key: 'writeSpeed', value: 'Write speed', width: undefined, colspan: undefined },
            { key: undefined,    value: 'Actions',     width: undefined, colspan: 2 }
        ];
        self.vMachines = ko.observableArray([]);
        self.vMachineGuids =  [];

        // Variables
        self.loadVMachinesHandle = undefined;

        // Functions
        self.load = function() {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.loadVMachinesHandle);
                self.loadVMachinesHandle = api.get('vmachines')
                    .done(function(data) {
                        var i, guids = [];
                        for (i = 0; i < data.length; i += 1) {
                            guids.push(data[i].guid);
                        }
                        for (i = 0; i < guids.length; i += 1) {
                            if ($.inArray(guids[i], self.vMachineGuids) === -1) {
                                self.vMachineGuids.push(guids[i]);
                                self.vMachines.push(new VMachine(guids[i]));
                            }
                        }
                        for (i = 0; i < self.vMachineGuids.length; i += 1) {
                            if ($.inArray(self.vMachineGuids[i], guids) === -1) {
                                self.vMachineGuids.splice(i, 1);
                                self.vMachines.splice(i, 1);
                            }
                        }
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };
        self.clone = function(guid) {
            var i, vms = self.vMachines();
            for (i = 0; i < vms.length; i += 1) {
                if (vms[i].guid() === guid) {
                    dialog.show(new CloneWizard({
                        modal: true,
                        machineguid: guid
                    }));
                }
            }
        };
        self.deleteVM = function(guid) {
            var i, vms = self.vMachines(), vm;
            for (i = 0; i < vms.length; i += 1) {
                if (vms[i].guid() === guid) {
                    vm = vms[i];
                }
            }
            if (vm !== undefined) {
                (function(vm) {
                    app.showMessage('Are you sure you want to delete "' + vm.name() + '"?', 'Are you sure?', ['Yes', 'No'])
                        .done(function(answer) {
                            if (answer === 'Yes') {
                                self.vMachines.destroy(vm);
                                generic.alertInfo('Marked for deletion', 'Machine ' + vm.name() + ' is marked for deletion...');
                                api.del('vmachines/' + vm.guid())
                                    .then(self.shared.tasks.wait)
                                    .done(function() {
                                        generic.alertSuccess('Machine deleted', 'Machine ' + vm.name() + ' deleted.');
                                    })
                                    .fail(function(error) {
                                        generic.alertSuccess('Error', 'Machine ' + vm.name() + ' could not be deleted: ' + error);
                                    });
                            }
                        });
                }(vm));
            }
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(self.load, 5000);
            self.refresher.run();
            self.refresher.start();
        };
        self.deactivate = function() {
            var i;
            for (i = 0; i < self.widgets.length; i += 2) {
                self.widgets[i].deactivate();
            }
            self.refresher.stop();
        };
    };
});