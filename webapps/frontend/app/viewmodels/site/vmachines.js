// license see http://www.openvstorage.com/licenses/opensource/
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
            { key: 'name',       value: $.t('ovs:generic.name'),       width: 150,       colspan: undefined },
            { key: undefined,    value: $.t('ovs:generic.vdisks'),     width: 60,        colspan: undefined },
            { key: 'storedData', value: $.t('ovs:generic.storeddata'), width: 100,       colspan: undefined },
            { key: 'cache',      value: $.t('ovs:generic.cache'),      width: 70,        colspan: undefined },
            { key: 'iops',       value: $.t('ovs:generic.iops'),       width: 55,        colspan: undefined },
            { key: 'readSpeed',  value: $.t('ovs:generic.readspeed'),  width: 100,       colspan: undefined },
            { key: 'writeSpeed', value: $.t('ovs:generic.writespeed'), width: undefined, colspan: undefined },
            { key: undefined,    value: $.t('ovs:generic.actions'),    width: undefined, colspan: 2 }
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
                    app.showMessage(
                            $.t('ovs:vmachines.suretodelete', { what: vm.name() }),
                            $.t('ovs:generic.areyousure'),
                            [$.t('ovs:generic.yes'), $.t('ovs:generic.no')]
                        )
                        .done(function(answer) {
                            if (answer === $.t('ovs:generic.yes')) {
                                self.vMachines.destroy(vm);
                                generic.alertInfo($.t('ovs:generic.vmachines.marked'), $.t('ovs:generic.vmachine.machinemarked', { what: vm.name() }));
                                api.del('vmachines/' + vm.guid())
                                    .then(self.shared.tasks.wait)
                                    .done(function() {
                                        generic.alertSuccess($.t('ovs:vmachines.deleted'), $.t('ovs:vmachines.machinedeleted', { what: vm.name() }));
                                    })
                                    .fail(function(error) {
                                        generic.alertSuccess($.t('ovs:generic.error'), 'Machine ' + vm.name() + ' could not be deleted: ' + error);
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
