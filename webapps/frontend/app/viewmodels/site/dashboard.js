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
    '../containers/vmachine', '../containers/vpool'
], function(ko, $, shared, generic, api, Refresher, VMachine, VPool) {
    "use strict";
    return function() {
        var self = this;

        // System
        self.shared    = shared;
        self.guard     = { authenticated: true };
        self.refresher = new Refresher();

        self.topItems            = 10;
        self.loadVsasHandle      = undefined;
        self.loadVPoolsHandle    = undefined;
        self.loadVMachinesHandle = undefined;

        self.vSAsLoading       = ko.observable(false);
        self.vPoolsLoading     = ko.observable(false);
        self.vMachinesLoading  = ko.observable(false);

        self.vSAs          = ko.observableArray([]);
        self.vPools        = ko.observableArray([]);
        self.vMachines     = ko.observableArray([]);

        self._cacheRatio = ko.computed(function() {
            var hits = 0, misses = 0, total, initialized = true, i, raw;
            for (i = 0; i < self.vPools().length; i += 1) {
                initialized = initialized && self.vPools()[i].cacheHits.initialized();
                initialized = initialized && self.vPools()[i].cacheMisses.initialized();
                hits += (self.vPools()[i].cacheHits.raw() || 0);
                misses += (self.vPools()[i].cacheMisses.raw() || 0);
            }
            total = hits + misses;
            if (total === 0) {
                total = 1;
            }
            raw = hits / total * 100;
            return {
                value: generic.formatRatio(raw),
                initialized: initialized,
                raw: raw
            };
        });
        self.cacheRatio = ko.computed(function() {
            return self._cacheRatio().value;
        });
        self.cacheRatio.initialized = ko.computed(function() {
            return self._cacheRatio().initialized;
        });
        self.cacheRatio.raw = ko.computed(function() {
            return self._cacheRatio().raw;
        });
        self._iops = ko.computed(function() {
            var total = 0, initialized = true, i;
            for (i = 0; i < self.vPools().length; i += 1) {
                initialized = initialized && self.vPools()[i].iops.initialized();
                total += (self.vPools()[i].iops.raw() || 0);
            }
            return {
                value: generic.formatNumber(total),
                initialized: initialized
            };
        });
        self.iops = ko.computed(function() {
            return self._iops().value;
        });
        self.iops.initialized = ko.computed(function() {
            return self._iops().initialized;
        });
        self._readSpeed = ko.computed(function() {
            var total = 0, initialized = true, i;
            for (i = 0; i < self.vPools().length; i += 1) {
                initialized = initialized && self.vPools()[i].readSpeed.initialized();
                total += (self.vPools()[i].readSpeed.raw() || 0);
            }
            return {
                value: generic.formatSpeed(total),
                initialized: initialized
            };
        });
        self.readSpeed = ko.computed(function() {
            return self._readSpeed().value;
        });
        self.readSpeed.initialized = ko.computed(function() {
            return self._readSpeed().initialized;
        });
        self._writeSpeed = ko.computed(function() {
            var total = 0, initialized = true, i;
            for (i = 0; i < self.vPools().length; i += 1) {
                initialized = initialized && self.vPools()[i].writeSpeed.initialized();
                total += (self.vPools()[i].writeSpeed.raw() || 0);
            }
            return {
                value: generic.formatSpeed(total),
                initialized: initialized
            };
        });
        self.writeSpeed = ko.computed(function() {
            return self._writeSpeed().value;
        });
        self.writeSpeed.initialized = ko.computed(function() {
            return self._writeSpeed().initialized;
        });

        self.topVpoolModes = ko.observableArray(['topstoreddata', 'topbandwidth']);
        self.topVPoolMode  = ko.observable('topstoreddata');
        self.topVPools     = ko.computed(function() {
            var vpools = [], result, i;
            self.vPools.sort(function(a, b) {
                if (self.topVPoolMode() === 'topstoreddata') {
                    result = (b.storedData.raw() || 0) - (a.storedData.raw() || 0);
                    return (result !== 0 ? result : generic.numberSort(a.name(), b.name()));
                }
                result = (
                    ((b.writeSpeed.raw() || 0) + (b.readSpeed.raw() || 0)) -
                    ((a.writeSpeed.raw() || 0) + (a.readSpeed.raw() || 0))
                );
                return (result !== 0 ? result : generic.numberSort(a.name(), b.name()));
            });
            for (i = 0; i < Math.min(self.topItems, self.vPools().length); i += 1) {
                vpools.push(self.vPools()[i]);
            }
            return vpools;
        }).extend({ throttle: 50 });

        self.topVmachineModes = ko.observableArray(['topstoreddata', 'topbandwidth']);
        self.topVmachineMode  = ko.observable('topstoreddata');
        self.topVmachines     = ko.computed(function() {
            var vmachines = [], result, i;
            self.vMachines.sort(function(a, b) {
                if (self.topVmachineMode() === 'topstoreddata') {
                    result = (b.storedData.raw() || 0) - (a.storedData.raw() || 0);
                    return (result !== 0 ? result : generic.numberSort(a.name(), b.name()));
                }
                result = (
                    ((b.writeSpeed.raw() || 0) + (b.readSpeed.raw() || 0)) -
                    ((a.writeSpeed.raw() || 0) + (a.readSpeed.raw() || 0))
                );
                return (result !== 0 ? result : generic.numberSort(a.name(), b.name()));
            });
            for (i = 0; i < Math.min(self.topItems, self.vMachines().length); i += 1) {
                vmachines.push(self.vMachines()[i]);
            }
            return vmachines;
        }).extend({ throttle: 50 });

        // Functions
        self.load = function() {
            return $.Deferred(function(deferred) {
                $.when.apply($, [
                        self.loadVsas(),
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
                if (generic.xhrCompleted(self.loadVMachinesHandle)) {
                    var query = {
                            query: {
                                type: 'AND',
                                items: [['is_internal', 'EQUALS', false],
                                        ['is_vtemplate', 'EQUALS', false],
                                        ['status', 'NOT_EQUALS', 'CREATED']]
                            }
                        };
                    self.loadVMachinesHandle = api.post('vmachines/filter', query, { full: true })
                        .done(function(data) {
                            var i, guids = [], vmdata = {}, vmachine;
                            for (i = 0; i < data.length; i += 1) {
                                guids.push(data[i].guid);
                                vmdata[data[i].guid] = data[i];
                            }
                            generic.crossFiller(
                                guids, self.vMachines,
                                function(guid) {
                                    return new VMachine(guid);
                                }, 'guid'
                            );
                            for (i = 0; i < self.vMachines().length; i += 1) {
                                // No reload, as we got all data in this call
                                vmachine = self.vMachines()[i];
                                vmachine.fillData(vmdata[vmachine.guid()]);
                            }
                            deferred.resolve();
                        })
                        .fail(deferred.reject)
                        .always(function() {
                            self.vMachinesLoading(false);
                        });
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.loadVPools = function() {
            return $.Deferred(function(deferred) {
                self.vPoolsLoading(true);
                if (generic.xhrCompleted(self.loadVPoolsHandle)) {
                    self.loadVPoolsHandle = api.get('vpools', undefined, { full: true })
                        .done(function(data) {
                            var i, guids = [], vpdata = {}, vpool;
                            for (i = 0; i < data.length; i += 1) {
                                guids.push(data[i].guid);
                                vpdata[data[i].guid] = data[i];
                            }
                            generic.crossFiller(
                                guids, self.vPools,
                                function(guid) {
                                    return new VPool(guid);
                                }, 'guid'
                            );
                            for (i = 0; i < self.vPools().length; i += 1) {
                                vpool = self.vPools()[i];
                                vpool.fillData(vpdata[vpool.guid()]);
                                vpool.load(true);
                            }
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
        self.loadVsas = function() {
            return $.Deferred(function(deferred) {
                self.vSAsLoading(true);
                if (generic.xhrCompleted(self.loadVsasHandle)) {
                    var query = {
                        query: {
                            type: 'AND',
                            items: [['is_internal', 'EQUALS', true]]
                        }
                    };
                    self.loadVsasHandle = api.post('vmachines/filter', query, { full: true })
                        .done(function(data) {
                            var i, guids = [], vmdata = {}, vmachine;
                            for (i = 0; i < data.length; i += 1) {
                                guids.push(data[i].guid);
                                vmdata[data[i].guid] = data[i];
                            }
                            generic.crossFiller(
                                guids, self.vSAs,
                                function(guid) {
                                    return new VMachine(guid);
                                }, 'guid'
                            );
                            for (i = 0; i < self.vSAs().length; i += 1) {
                                vmachine = self.vSAs()[i];
                                vmachine.fillData(vmdata[vmachine.guid()]);
                                vmachine.load(false, true);
                            }
                            deferred.resolve();
                        })
                        .fail(deferred.reject)
                        .always(function() {
                            self.vSAsLoading(false);
                        });
                } else {
                    deferred.reject();
                }
            }).promise();
        };


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
