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
    'knockout', 'jquery',
    'ovs/shared', 'ovs/generic', 'ovs/api', 'ovs/refresher',
    '../containers/vmachine', '../containers/vpool', '../containers/storagerouter'
], function(ko, $, shared, generic, api, Refresher, VMachine, VPool, StorageRouter) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared    = shared;
        self.guard     = { authenticated: true };
        self.refresher = new Refresher();
        self.topItems  = 10;
        self.query     = {
            type: 'AND',
            items: [['is_vtemplate', 'EQUALS', false],
                    ['status', 'NOT_EQUALS', 'CREATED']]
        };

        // Handles
        self.loadStorageRoutersHandle = undefined;
        self.loadVPoolsHandle         = undefined;
        self.loadVMachinesHandle      = undefined;
        self.loadVMachineGuidsHandle  = undefined;

        // Observ ables
        self.storageRoutersLoading = ko.observable(false);
        self.vPoolsLoading         = ko.observable(false);
        self.vMachinesLoading      = ko.observable(false);
        self.topVPoolMode          = ko.observable('topstoreddata');
        self.topVmachineMode       = ko.observable('topstoreddata');
        self.storageRouters        = ko.observableArray([]);
        self.vPools                = ko.observableArray([]);
        self.vMachineGuids         = ko.observableArray([]);
        self.topVMachines          = ko.observableArray([]);
        self.topVpoolModes         = ko.observableArray(['topstoreddata', 'topbandwidth']);
        self.topVmachineModes      = ko.observableArray(['topstoreddata', 'topbandwidth']);

        // Computed
        self.topVPools = ko.computed(function() {
            return self.vPools.slice(0, 10);
        });
        self._cacheRatio = ko.computed(function() {
            var hits = 0, misses = 0, total, initialized = true, i, raw;
            $.each(self.vPools(), function(index, vpool) {
                hits += (vpool.cacheHits.raw() || 0);
                misses += (vpool.cacheMisses.raw() || 0);
            });
            total = hits + misses;
            if (total === 0) {
                total = 1;
            }
            raw = hits / total * 100;
            return {
                value: generic.formatRatio(raw),
                raw: raw
            };
        });
        self.cacheRatio = ko.computed(function() {
            return self._cacheRatio().value;
        });
        self.cacheRatio.raw = ko.computed(function() {
            return self._cacheRatio().raw;
        });
        self._iops = ko.computed(function() {
            var total = 0;
            $.each(self.vPools(), function(index, vpool) {
                total += (vpool.iops.raw() || 0);
            });
            return {
                value: generic.formatNumber(total),
                raw: total
            };
        });
        self.iops = ko.computed(function() {
            return self._iops().value;
        });
        self.iops.raw = ko.computed(function() {
            return self._iops().raw;
        });
        self.readSpeed = ko.computed(function() {
            var total = 0;
            $.each(self.vPools(), function(index, vpool) {
                total += (vpool.readSpeed.raw() || 0);
            });
            return generic.formatSpeed(total);
        });
        self.writeSpeed = ko.computed(function() {
            var total = 0;
            $.each(self.vPools(), function(index, vpool) {
                total += (vpool.writeSpeed.raw() || 0);
            });
            return generic.formatSpeed(total);
        });

        // Functions
        self.load = function() {
            return $.Deferred(function(deferred) {
                $.when.apply($, [
                        self.loadStorageRouters(),
                        self.loadVPools(),
                        self.loadVMachines()
                    ])
                    .done(deferred.resolve)
                    .fail(deferred.reject);
            }).promise();
        };
        self.loadVMachines = function() {
            return $.Deferred(function(deferred) {
                self.vMachinesLoading(true);
                $.when(
                        $.Deferred(function(vms_dfr) {
                            if (generic.xhrCompleted(self.loadVMachinesHandle)) {
                                var filter = {
                                    contents: 'statistics,stored_data',
                                    sort: (self.topVmachineMode() === 'topstoreddata' ? '-stored_data,name' : '-statistics[data_transferred_ps],name'),
                                    page: 1,
                                    query: JSON.stringify(self.query)
                                };
                                self.loadVMachinesHandle = api.get('vmachines', { queryparams: filter })
                                    .done(function(data) {
                                        var vms = [], vm;
                                        $.each(data.data, function(index, vmdata) {
                                            vm = new VMachine(vmdata.guid);
                                            vm.fillData(vmdata);
                                            vms.push(vm);
                                        });
                                        self.topVMachines(vms);
                                        vms_dfr.resolve();
                                    })
                                    .fail(vms_dfr.reject);
                            } else {
                                vms_dfr.reject();
                            }
                        }).promise(),
                        $.Deferred(function(vmg_dfr) {
                            if (generic.xhrCompleted(self.loadVMachineGuidsHandle)) {
                                self.loadVMachineGuidsHandle = api.get('vmachines', { queryparams: { query: JSON.stringify(self.query) } })
                                    .done(function(data) {
                                        self.vMachineGuids(data.data);
                                        vmg_dfr.resolve();
                                    })
                                    .fail(vmg_dfr.reject);
                            } else {
                                vmg_dfr.reject();
                            }
                        }).promise()
                    )
                    .done(function() {
                        self.vMachinesLoading(false);
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };
        self.loadVPools = function() {
            return $.Deferred(function(deferred) {
                self.vPoolsLoading(true);
                if (generic.xhrCompleted(self.loadVPoolsHandle)) {
                    var filter = {
                        contents: 'statistics,stored_data',
                        sort: (self.topVPoolMode() === 'topstoreddata' ? '-stored_data,name' : '-statistics[data_transferred_ps],name')
                    };
                    self.loadVPoolsHandle = api.get('vpools', { queryparams: filter })
                        .done(function(data) {
                            var vpools = [], vpool;
                            $.each(data.data, function(index, vpdata) {
                                vpool = new VPool(vpdata.guid);
                                vpool.fillData(vpdata);
                                vpools.push(vpool);
                            });
                            self.vPools(vpools);
                            deferred.resolve();
                        })
                        .fail(deferred.reject)
                        .always(function() {
                            self.vPoolsLoading(false);
                        });
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.loadStorageRouters = function() {
            return $.Deferred(function(deferred) {
                self.storageRoutersLoading(true);
                if (generic.xhrCompleted(self.loadStorageRoutersHandle)) {
                    self.loadStorageRoutersHandle = api.get('storagerouters', {
                        queryparams: {
                            contents: 'status,vdisks_guids',
                            sort: 'name,vdisks_guids'
                        }
                    })
                        .done(function(data) {
                            var guids = [], sadata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                sadata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.storageRouters,
                                function(guid) {
                                    return new StorageRouter(guid);
                                }, 'guid'
                            );
                            $.each(self.storageRouters(), function(index, storageRouter) {
                                storageRouter.fillData(sadata[storageRouter.guid()]);
                            });
                            deferred.resolve();
                        })
                        .fail(deferred.reject)
                        .always(function() {
                            self.storageRoutersLoading(false);
                        });
                } else {
                    deferred.reject();
                }
            }).promise();
        };

        // Subscriptions
        self.topVmachineMode.subscribe(function() {
            self.loadVMachines();
        });
        self.topVPoolMode.subscribe(function() {
            self.loadVPools();
        });

        // Durandal
        self.activate = function() {
            self.refresher.init(self.load, 5000);
            self.refresher.run();
            self.refresher.start();
            self.shared.footerData(self.vPools);
        };
        self.deactivate = function() {
            self.refresher.stop();
            self.shared.footerData(ko.observable());
        };
    };
});
