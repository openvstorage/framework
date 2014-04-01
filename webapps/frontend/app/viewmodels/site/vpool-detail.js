// Copyright 2014 CloudFounders NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define([
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vpool', '../containers/vmachine',
    '../wizards/vsatovpool/index'
], function($, app, dialog, ko, shared, generic, Refresher, api, VPool, VMachine, VSAToVPoolWizard) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared          = shared;
        self.guard           = { authenticated: true };
        self.refresher       = new Refresher();
        self.widgets         = [];
        self.vSACache        = {};
        self.vMachineCache   = {};
        self.vDiskHeaders    = [
            { key: 'name',         value: $.t('ovs:generic.name'),       width: undefined },
            { key: 'vmachine',     value: $.t('ovs:generic.vmachine'),   width: 110       },
            { key: 'size',         value: $.t('ovs:generic.size'),       width: 100       },
            { key: 'storedData',   value: $.t('ovs:generic.storeddata'), width: 110       },
            { key: 'cacheRatio',   value: $.t('ovs:generic.cache'),      width: 100       },
            { key: 'iops',         value: $.t('ovs:generic.iops'),       width: 55        },
            { key: 'readSpeed',    value: $.t('ovs:generic.read'),       width: 100       },
            { key: 'writeSpeed',   value: $.t('ovs:generic.write'),      width: 100       },
            { key: 'failoverMode', value: $.t('ovs:generic.focstatus'),  width: 50        }
        ];
        self.vMachineHeaders = [
            { key: 'name',         value: $.t('ovs:generic.name'),       width: undefined },
            { key: 'vsa',          value: $.t('ovs:generic.vsa'),        width: 100       },
            { key: undefined,      value: $.t('ovs:generic.vdisks'),     width: 60        },
            { key: 'storedData',   value: $.t('ovs:generic.storeddata'), width: 110       },
            { key: 'cacheRatio',   value: $.t('ovs:generic.cache'),      width: 100       },
            { key: 'iops',         value: $.t('ovs:generic.iops'),       width: 55        },
            { key: 'readSpeed',    value: $.t('ovs:generic.read'),       width: 120       },
            { key: 'writeSpeed',   value: $.t('ovs:generic.write'),      width: 120       },
            { key: 'failoverMode', value: $.t('ovs:generic.focstatus'),  width: 50        }
        ];

        // Handles
        self.loadVDisksHandle       = undefined;
        self.refreshVDisksHandle    = {};
        self.loadVMachinesHandle    = undefined;
        self.refreshVMachinesHandle = {};
        self.loadVSAsHandle         = undefined;

        // Observables
        self.vDisksInitialLoad    = ko.observable(true);
        self.vMachinesInitialLoad = ko.observable(true);
        self.vSAsLoaded           = ko.observable(false);
        self.addingVSAs           = ko.observable(false);
        self.vPool                = ko.observable();
        self.vSAs                 = ko.observableArray([]);
        self.checkedVSAGuids      = ko.observableArray([]);

        // Computed
        self.pendingVSAs = ko.computed(function() {
            var vsas = [];
            $.each(self.vSAs(), function(index, vsa) {
                if ($.inArray(vsa.guid(), self.checkedVSAGuids()) !== -1 && $.inArray(vsa.guid(), self.vPool().servingVSAGuids()) === -1) {
                    vsas.push(vsa);
                }
            });
            return vsas;
        });

        // Functions
        self.load = function() {
            return $.Deferred(function (deferred) {
                var vpool = self.vPool();
                vpool.load('vsrs', { skipDisks: true })
                    .then(function() {
                        self.vDisksInitialLoad(false);
                        self.vMachinesInitialLoad(false);
                    })
                    .then(vpool.loadVDisks)
                    .then(vpool.loadServingVSAs)
                    .then(self.loadVSAs)
                    .then(function() {
                        if (self.checkedVSAGuids().length === 0) {
                            self.checkedVSAGuids(self.vPool().servingVSAGuids());
                        }
                    })
                    .always(deferred.resolve);
            }).promise();
        };
        self.loadVSAs = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadVSAsHandle)) {
                    var query, options;
                    query = {
                        query: {
                            type: 'AND',
                            items: [['is_internal', 'EQUALS', true]]
                        }
                    };
                    options = {
                        sort: 'name',
                        full: true,
                        contents: ''
                    };
                    self.loadVSAsHandle = api.post('vmachines/filter', query, options)
                        .done(function(data) {
                            var guids = [], vsadata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                vsadata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.vSAs,
                                function(guid) {
                                    var vmachine = new VMachine(guid);
                                    vmachine.fillData(vsadata[guid]);
                                    return vmachine;
                                }, 'guid'
                            );
                            self.vSAsLoaded(true);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.refreshVDisks = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.refreshVDisksHandle[page])) {
                    var options = {
                        sort: 'devicename',
                        full: true,
                        page: page,
                        contents: '_dynamics,_relations,-snapshots',
                        vpoolguid: self.vPool().guid()
                    };
                    self.refreshVDisksHandle[page] = api.get('vdisks', {}, options)
                        .done(function(data) {
                            var guids = [], vddata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                vddata[item.guid] = item;
                            });
                            $.each(self.vPool().vDisks(), function(index, vdisk) {
                                if ($.inArray(vdisk.guid(), guids) !== -1) {
                                    vdisk.fillData(vddata[vdisk.guid()]);
                                    var vm, vMachineGuid = vdisk.vMachineGuid();
                                    if (vMachineGuid && (vdisk.vMachine() === undefined || vdisk.vMachine().guid() !== vMachineGuid)) {
                                        if (!self.vMachineCache.hasOwnProperty(vMachineGuid)) {
                                            vm = new VMachine(vMachineGuid);
                                            vm.load('');
                                            self.vMachineCache[vMachineGuid] = vm;
                                        }
                                        vdisk.vMachine(self.vMachineCache[vMachineGuid]);
                                    }
                                }
                            });
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.refreshVMachines = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.refreshVMachinesHandle[page])) {
                    var options = {
                        sort: 'name',
                        full: true,
                        page: page,
                        contents: '_dynamics,-snapshots,-hypervisor_status',
                        vpoolguid: self.vPool().guid()
                    };
                    self.refreshVMachinesHandle[page] = api.get('vmachines', {}, options)
                        .done(function(data) {
                            var guids = [], vmdata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                vmdata[item.guid] = item;
                            });
                            $.each(self.vPool().vMachines(), function(index, vmachine) {
                                if ($.inArray(vmachine.guid(), guids) !== -1) {
                                    if (!self.vMachineCache.hasOwnProperty(vmachine.guid())) {
                                        self.vMachineCache[vmachine.guid()] = vmachine;
                                    }
                                    vmachine.fillData(vmdata[vmachine.guid()]);
                                    generic.crossFiller(
                                        vmachine.vSAGuids, vmachine.vSAs,
                                        function(guid) {
                                            if (!self.vSACache.hasOwnProperty(guid)) {
                                                var vm = new VMachine(guid);
                                                vm.load('');
                                                self.vSACache[guid] = vm;
                                            }
                                            return self.vSACache[guid];
                                        }, 'guid'
                                    );
                                }
                            });
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.sync = function() {
            if (self.vPool() !== undefined) {
                var vp = self.vPool();
                app.showMessage(
                        $.t('ovs:vpools.sync.warning'),
                        $.t('ovs:vpools.sync.title', { what: vp.name() }),
                        [$.t('ovs:vpools.sync.no'), $.t('ovs:vpools.sync.yes')]
                    )
                    .done(function(answer) {
                        if (answer === $.t('ovs:vpools.sync.yes')) {
                            generic.alertInfo(
                                $.t('ovs:vpools.sync.marked'),
                                $.t('ovs:vpools.sync.markedmsg', { what: vp.name() })
                            );
                            api.post('vpools/' + vp.guid() + '/sync_vmachines')
                                .then(self.shared.tasks.wait)
                                .done(function() {
                                    generic.alertSuccess(
                                        $.t('ovs:vpools.sync.done'),
                                        $.t('ovs:vpools.sync.donemsg', { what: vp.name() })
                                    );
                                })
                                .fail(function(error) {
                                    generic.alertError(
                                        $.t('ovs:generic.error'),
                                        $.t('ovs:generic.messages.errorwhile', {
                                            context: 'error',
                                            what: $.t('ovs:vpools.sync.errormsg', { what: vp.name() }),
                                            error: error
                                        })
                                    );
                                });
                        }
                    });
            }
        };
        self.updateVSAServing = function() {
            self.addingVSAs(true);
            var deferred = $.Deferred(), wizard;
            wizard = new VSAToVPoolWizard({
                modal: true,
                completed: deferred,
                vPool: self.vPool(),
                vSAs: self.pendingVSAs
            });
            wizard.closing.always(function() {
                deferred.resolve();
            });
            dialog.show(wizard);
            deferred.always(function() {
                self.addingVSAs(false);
            });
        };

        // Durandal
        self.activate = function(mode, guid) {
            self.vPool(new VPool(guid));
            self.refresher.init(self.load, 5000);
            self.refresher.run();
            self.refresher.start();
            self.shared.footerData(self.vPool);
        };
        self.deactivate = function() {
            $.each(self.widgets, function(index, item) {
                item.deactivate();
            });
            self.refresher.stop();
            self.shared.footerData(ko.observable());
        };
    };
});
