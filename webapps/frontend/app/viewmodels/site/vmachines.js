// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vmachine', '../containers/vpool', '../wizards/clone/index', '../wizards/snapshot/index'
], function($, app, dialog, ko, shared, generic, Refresher, api, VMachine, VPool, CloneWizard, SnapshotWizard) {
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
            { key: 'name',         value: $.t('ovs:generic.name'),       width: 150,       colspan: undefined },
            { key: 'vpool',        value: $.t('ovs:generic.vpool'),      width: 150,       colspan: undefined },
            { key: 'vsa',          value: $.t('ovs:generic.vsa'),        width: 150,       colspan: undefined },
            { key: undefined,      value: $.t('ovs:generic.vdisks'),     width: 60,        colspan: undefined },
            { key: 'storedData',   value: $.t('ovs:generic.storeddata'), width: 110,       colspan: undefined },
            { key: 'cacheRatio',   value: $.t('ovs:generic.cache'),      width: 100,       colspan: undefined },
            { key: 'iops',         value: $.t('ovs:generic.iops'),       width: 55,        colspan: undefined },
            { key: 'readSpeed',    value: $.t('ovs:generic.readspeed'),  width: 100,       colspan: undefined },
            { key: 'writeSpeed',   value: $.t('ovs:generic.writespeed'), width: 100,       colspan: undefined },
            { key: 'failoverMode', value: $.t('ovs:generic.focstatus'),  width: undefined, colspan: undefined },
            { key: undefined,      value: $.t('ovs:generic.actions'),    width: 80,        colspan: undefined }
        ];
        self.vMachines = ko.observableArray([]);
        self.vMachineGuids =  [];

        // Variables
        self.loadVMachinesHandle = undefined;

        // Functions
        self.vpoolUrl = function(guid) {
            return '#' + self.shared.mode() + '/vpool/' + (guid.call ? guid() : guid);
        };
        self.vmachineUrl = function(guid) {
            return '#' + self.shared.mode() + '/vmachine/' + (guid.call ? guid() : guid);
        };
        self.load = function() {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.loadVMachinesHandle);
                var query = {
                        query: {
                            type: 'AND',
                            items: [['is_internal', 'EQUALS', false]]
                        }
                    };
                self.loadVMachinesHandle = api.post('vmachines/filter', query)
                    .done(function(data) {
                        var i, guids = [];
                        for (i = 0; i < data.length; i += 1) {
                            guids.push(data[i].guid);
                        }
                        generic.crossFiller(
                            guids, self.vMachineGuids, self.vMachines,
                            function(guid) {
                                return new VMachine(guid);
                            }
                        );
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };
        self.loadVMachine = function(vm) {
            $.when.apply($, [
                    vm.load(),
                    vm.fetchVSAGuids(),
                    vm.fetchVPoolGuids()
                ])
                .done(function() {
                    // Merge in the VSAs
                    var i, currentGuids = [];
                    for (i = 0; i < vm.vsas().length; i += 1) {
                        currentGuids.push(vm.vsas()[i].guid());
                    }
                    generic.crossFiller(
                        vm.vsaGuids, currentGuids, vm.vsas,
                        function(guid) {
                            var vm = new VMachine(guid);
                            vm.load();
                            return vm;
                        }
                    );
                    // Merge in the vPools
                    currentGuids = [];
                    for (i = 0; i < vm.vpools().length; i += 1) {
                        currentGuids.push(vm.vpools()[i].guid());
                    }
                    generic.crossFiller(
                        vm.vPoolGuids, currentGuids, vm.vpools,
                        function(guid) {
                            var vm = new VPool(guid);
                            vm.load();
                            return vm;
                        }
                    );
                });
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
        self.snapshot = function(guid) {
            var i, vms = self.vMachines();
            for (i = 0; i < vms.length; i += 1) {
                if (vms[i].guid() === guid) {
                    dialog.show(new SnapshotWizard({
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
                                generic.alertInfo($.t('ovs:vmachines.marked'), $.t('ovs:vmachines.machinemarked', { what: vm.name() }));
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
