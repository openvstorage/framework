// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vmachine', '../containers/vpool', '../wizards/rollback/index', '../wizards/snapshot/index'
], function($, app, dialog, ko, shared, generic, Refresher, api, VMachine, VPool, RollbackWizard, SnapshotWizard) {
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
            { key: 'readSpeed',    value: $.t('ovs:generic.read'),       width: 100,       colspan: undefined },
            { key: 'writeSpeed',   value: $.t('ovs:generic.write'),      width: 100,       colspan: undefined },
            { key: 'failoverMode', value: $.t('ovs:generic.focstatus'),  width: undefined, colspan: undefined },
            { key: undefined,      value: $.t('ovs:generic.actions'),    width: 100,       colspan: undefined }
        ];
        self.vMachines = ko.observableArray([]);
        self.vMachineGuids =  [];

        // Variables
        self.loadVMachinesHandle = undefined;

        // Functions
        self.fetchVMachines = function() {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.loadVMachinesHandle);
                var query = {
                        query: {
                            type: 'AND',
                            items: [['is_internal', 'EQUALS', false],
                                    ['is_vtemplate', 'EQUALS', false]]
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
        self.rollback = function(guid) {
            dialog.show(new RollbackWizard({
                modal: true,
                type: 'vmachine',
                guid: guid
            }));
        };
        self.snapshot = function(guid) {
            dialog.show(new SnapshotWizard({
                modal: true,
                machineguid: guid
            }));
        };
        self.setAsTemplate = function(guid) {
            var i, vms = self.vMachines(), vm;
            for (i = 0; i < vms.length; i += 1) {
                if (vms[i].guid() === guid) {
                    vm = vms[i];
                }
            }
            if (vm !== undefined) {
                app.showMessage(
                        $.t('ovs:vmachines.setastemplate.warning'),
                        $.t('ovs:vmachines.setastemplate.title', { what: vm.name() }),
                        [$.t('ovs:vmachines.setastemplate.no'), $.t('ovs:vmachines.setastemplate.yes')]
                    )
                    .done(function(answer) {
                        if (answer === $.t('ovs:vmachines.setastemplate.yes')) {
                            generic.alertInfo(
                                $.t('ovs:vmachines.setastemplate.marked'),
                                $.t('ovs:vmachines.setastemplate.markedmsg', { what: vm.name() })
                            );
                            api.post('vmachines/' + vm.guid() + '/set_as_template')
                                .then(self.shared.tasks.wait)
                                .done(function() {
                                    self.vMachines.destroy(vm);
                                    generic.alertSuccess(
                                        $.t('ovs:vmachines.setastemplate.done'),
                                        $.t('ovs:vmachines.setastemplate.donemsg', { what: vm.name() })
                                    );
                                })
                                .fail(function(error) {
                                    generic.alertError(
                                        $.t('ovs:generic.error'),
                                        $.t('ovs:generic.errorwhile', {
                                            context: 'error',
                                            what: $.t('ovs:vmachines.setastemplate.errormsg', { what: vm.name() }),
                                            error: error
                                        })
                                    );
                                });
                        }
                    });
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
                app.showMessage(
                        $.t('ovs:vmachines.delete.warning', { what: vm.name() }),
                        $.t('ovs:generic.areyousure'),
                        [$.t('ovs:generic.no'), $.t('ovs:generic.yes')]
                    )
                    .done(function(answer) {
                        if (answer === $.t('ovs:generic.yes')) {
                            self.vMachines.destroy(vm);
                            generic.alertInfo(
                                $.t('ovs:vmachines.delete.marked'),
                                $.t('ovs:vmachines.delete.markedmsg', { what: vm.name() })
                            );
                            api.del('vmachines/' + vm.guid())
                                .then(self.shared.tasks.wait)
                                .done(function() {
                                    generic.alertSuccess(
                                        $.t('ovs:vmachines.delete.done'),
                                        $.t('ovs:vmachines.delete.donemsg', { what: vm.name() })
                                    );
                                })
                                .fail(function(error) {
                                    generic.alertError(
                                        $.t('ovs:generic.error'),
                                        $.t('ovs:generic.errorwhile', {
                                            context: 'error',
                                            what: $.t('ovs:vmachines.delete.errormsg', { what: vm.name() }),
                                            error: error
                                        })
                                    );
                                });
                        }
                    });
            }
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(self.fetchVMachines, 5000);
            self.refresher.run();
            self.refresher.start();
            self.shared.footerData(self.vMachines);
        };
        self.deactivate = function() {
            var i;
            for (i = 0; i < self.widgets.length; i += 2) {
                self.widgets[i].deactivate();
            }
            self.refresher.stop();
            self.shared.footerData(ko.observable());
        };
    };
});
