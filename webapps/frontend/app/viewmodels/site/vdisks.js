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
/*global define, window */
define([
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vdisk', '../containers/vmachine', '../containers/vpool', '../wizards/rollback/index'
], function($, app, dialog, ko, shared, generic, Refresher, api, VDisk, VMachine, VPool, RollbackWizard) {
    "use strict";
    return function() {
        var self = this;

        // System
        self.shared      = shared;
        self.guard       = { authenticated: true };
        self.refresher   = new Refresher();
        self.widgets     = [];
        self.updateSort  = false;
        self.sortTimeout = undefined;

        // Data
        self.vDiskHeaders = [
            { key: 'name',         value: $.t('ovs:generic.name'),         width: 150,       colspan: undefined },
            { key: 'vmachine',     value: $.t('ovs:generic.vmachine'),     width: 110,       colspan: undefined },
            { key: 'vpool',        value: $.t('ovs:generic.vpool'),        width: 110,       colspan: undefined },
            { key: 'vsa',          value: $.t('ovs:generic.vsa'),          width: 110,       colspan: undefined },
            { key: 'size',         value: $.t('ovs:generic.size'),         width: 100,       colspan: undefined },
            { key: 'storedData',   value: $.t('ovs:generic.storeddata'),   width: 110,       colspan: undefined },
            { key: 'cacheRatio',   value: $.t('ovs:generic.cache'),        width: 100,       colspan: undefined },
            { key: 'iops',         value: $.t('ovs:generic.iops'),         width: 55,        colspan: undefined },
            { key: 'readSpeed',    value: $.t('ovs:generic.read'),         width: 100,       colspan: undefined },
            { key: 'writeSpeed',   value: $.t('ovs:generic.write'),        width: 100,       colspan: undefined },
            { key: 'failoverMode', value: $.t('ovs:generic.focstatus'),    width: undefined, colspan: undefined },
            { key: undefined,      value: $.t('ovs:generic.actions'),      width: 80,        colspan: undefined }
        ];
        self.vDisks = ko.observableArray([]);
        self.vDiskGuids = [];
        self.vMachineCache = {};
        self.vPoolCache = {};
        self.vsaCache = {};

        // Variables
        self.loadVDisksHandle = undefined;

        // Functions
        self.load = function() {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.loadVDisksHandle);
                self.loadVDisksHandle = api.get('vdisks')
                    .done(function(data) {
                        var i, guids = [];
                        for (i = 0; i < data.length; i += 1) {
                            guids.push(data[i].guid);
                        }
                        generic.crossFiller(
                            guids, self.vDiskGuids, self.vDisks,
                            function(guid) {
                                return new VDisk(guid);
                            }, 'guid'
                        );
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };
        self.loadVDisk = function(vdisk, reduced) {
            reduced = reduced || false;
            return $.Deferred(function(deferred) {
                var calls = [vdisk.load()];
                if (!reduced) {
                    calls.push(vdisk.fetchVSAGuid());
                }
                $.when.apply($, calls)
                    .done(function() {
                        var vm, pool,
                            vsaGuid = vdisk.vsaGuid(),
                            vMachineGuid = vdisk.vMachineGuid(),
                            vPoolGuid = vdisk.vpoolGuid();
                        if (vsaGuid && (vdisk.vsa() === undefined || vdisk.vsa().guid() !== vsaGuid)) {
                            if (!self.vsaCache.hasOwnProperty(vsaGuid)) {
                                vm = new VMachine(vsaGuid);
                                vm.load();
                                self.vsaCache[vsaGuid] = vm;
                            }
                            vdisk.vsa(self.vsaCache[vsaGuid]);
                        }
                        if (vMachineGuid && (vdisk.vMachine() === undefined || vdisk.vMachine().guid() !== vMachineGuid)) {
                            if (!self.vMachineCache.hasOwnProperty(vMachineGuid)) {
                                vm = new VMachine(vMachineGuid);
                                vm.load();
                                self.vMachineCache[vMachineGuid] = vm;
                            }
                            vdisk.vMachine(self.vMachineCache[vMachineGuid]);
                        }
                        if (vPoolGuid && (vdisk.vpool() === undefined || vdisk.vpool().guid() !== vPoolGuid)) {
                            if (!self.vPoolCache.hasOwnProperty(vPoolGuid)) {
                                pool = new VPool(vPoolGuid);
                                pool.load();
                                self.vPoolCache[vPoolGuid] = pool;
                            }
                            vdisk.vpool(self.vPoolCache[vPoolGuid]);
                        }
                        // (Re)sort vDisks
                        if (self.updateSort) {
                            self.sort();
                        }
                    })
                    .always(deferred.resolve);
            }).promise();
        };
        self.sort = function() {
            if (self.sortTimeout) {
                window.clearTimeout(self.sortTimeout);
            }
            self.sortTimeout = window.setTimeout(function() { generic.advancedSort(self.vDisks, ['name', 'guid']); }, 250);
        };

        self.rollback = function(guid) {
            var i, vds = self.vDisks(), vd;
            for (i = 0; i < vds.length; i += 1) {
                if (vds[i].guid() === guid) {
                    vd = vds[i];
                }
            }
            if (vd.vMachine() === undefined || !vd.vMachine().isRunning()) {
                dialog.show(new RollbackWizard({
                    modal: true,
                    type: 'vdisk',
                    guid: guid
                }));
            }
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(self.load, 5000);
            self.refresher.start();
            self.shared.footerData(self.vDisks);

            var loads = [];
            self.load()
                .done(function() {
                    var i, vdisks = self.vDisks();
                    for (i = 0; i < vdisks.length; i += 1) {
                        loads.push(self.loadVDisk(vdisks[i], true));
                    }
                });
            $.when.apply($, loads)
                .done(function() {
                    self.sort();
                    self.updateSort = true;
                });
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
