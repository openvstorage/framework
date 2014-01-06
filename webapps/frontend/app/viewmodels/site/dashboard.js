// license see http://www.openvstorage.com/licenses/opensource/
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

        self.vsasLoading       = ko.observable(false);
        self.vPoolsLoading     = ko.observable(false);
        self.vMachinesLoading  = ko.observable(false);

        self.vsaGuids      = [];
        self.vsas          = ko.observableArray([]);
        self.vpoolGuids    = [];
        self.vpools        = ko.observableArray([]);
        self.vmachineGuids = [];
        self.vmachines     = ko.observableArray([]);

        self._cacheRatio = ko.computed(function() {
            var hits = 0, misses = 0, total, initialized = true, i, raw;
            for (i = 0; i < self.vpools().length; i += 1) {
                initialized = initialized && self.vpools()[i].cacheHits.initialized();
                initialized = initialized && self.vpools()[i].cacheMisses.initialized();
                hits += (self.vpools()[i].cacheHits.raw() || 0);
                misses += (self.vpools()[i].cacheMisses.raw() || 0);
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
            for (i = 0; i < self.vpools().length; i += 1) {
                initialized = initialized && self.vpools()[i].iops.initialized();
                total += (self.vpools()[i].iops.raw() || 0);
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
            for (i = 0; i < self.vpools().length; i += 1) {
                initialized = initialized && self.vpools()[i].readSpeed.initialized();
                total += (self.vpools()[i].readSpeed.raw() || 0);
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
            for (i = 0; i < self.vpools().length; i += 1) {
                initialized = initialized && self.vpools()[i].writeSpeed.initialized();
                total += (self.vpools()[i].writeSpeed.raw() || 0);
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
            var vpools = [], i;
            self.vpools.sort(function(a, b) {
                if (self.topVPoolMode() === 'topstoreddata') {
                    return ((b.storedData.raw() || 0) - (a.storedData.raw() || 0));
                }
                return (
                    ((b.writeSpeed.raw() || 0) + (b.readSpeed.raw() || 0)) -
                    ((a.writeSpeed.raw() || 0) + (a.readSpeed.raw() || 0))
                );
            });
            for (i = 0; i < Math.min(self.topItems, self.vpools().length); i += 1) {
                vpools.push(self.vpools()[i]);
            }
            return vpools;
        });

        self.topVmachineModes = ko.observableArray(['topstoreddata', 'topbandwidth']);
        self.topVmachineMode  = ko.observable('topstoreddata');
        self.topVmachines     = ko.computed(function() {
            var vmachines = [], i;
            self.vmachines.sort(function(a, b) {
                if (self.topVmachineMode() === 'topstoreddata') {
                    return ((b.storedData.raw() || 0) - (a.storedData.raw() || 0) );
                }
                return (
                    ((b.writeSpeed.raw() || 0) + (b.readSpeed.raw() || 0)) -
                    ((a.writeSpeed.raw() || 0) + (a.readSpeed.raw() || 0))
                );
            });
            for (i = 0; i < Math.min(self.topItems, self.vmachines().length); i += 1) {
                vmachines.push(self.vmachines()[i]);
            }
            return vmachines;
        });

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
                generic.xhrAbort(self.loadVMachinesHandle);
                var query = {
                        query: {
                            type: 'AND',
                            items: [['is_internal', 'EQUALS', false],
                                    ['is_vtemplate', 'EQUALS', false],
                                    ['status', 'NOT_EQUALS', 'CREATED']]
                        }
                    };
                self.loadVMachinesHandle = api.post('vmachines/filter', query)
                    .done(function(data) {
                        var i, guids = [];
                        for (i = 0; i < data.length; i += 1) {
                            guids.push(data[i].guid);
                        }
                        generic.crossFiller(
                            guids, self.vmachineGuids, self.vmachines,
                            function(guid) {
                                return new VMachine(guid);
                            }, 'guid'
                        );
                        for (i = 0; i < self.vmachines().length; i += 1) {
                            self.vmachines()[i].load();
                        }
                        deferred.resolve();
                    })
                    .fail(deferred.reject)
                    .always(function() {
                        self.vMachinesLoading(false);
                    });
            }).promise();
        };
        self.loadVPools = function() {
            return $.Deferred(function(deferred) {
                self.vPoolsLoading(true);
                generic.xhrAbort(self.loadVPoolsHandle);
                self.loadVPoolsHandle = api.get('vpools')
                    .done(function(data) {
                        var i, guids = [];
                        for (i = 0; i < data.length; i += 1) {
                            guids.push(data[i].guid);
                        }
                        generic.crossFiller(
                            guids, self.vpoolGuids, self.vpools,
                            function(guid) {
                                return new VPool(guid);
                            }, 'guid'
                        );
                        for (i = 0; i < self.vpools().length; i += 1) {
                            self.vpools()[i].load();
                        }
                        deferred.resolve();
                    })
                    .fail(deferred.reject)
                    .always(function() {
                        self.vPoolsLoading(false);
                    });
            }).promise();
        };
        self.loadVsas = function() {
            return $.Deferred(function(deferred) {
                self.vsasLoading(true);
                generic.xhrAbort(self.loadVsasHandle);
                var query = {
                    query: {
                        type: 'AND',
                        items: [['is_internal', 'EQUALS', true]]
                    }
                };
                self.loadVsasHandle = api.post('vmachines/filter', query)
                    .done(function(data) {
                        var i, guids = [];
                        for (i = 0; i < data.length; i += 1) {
                            guids.push(data[i].guid);
                        }
                        generic.crossFiller(
                            guids, self.vsaGuids, self.vsas,
                            function(guid) {
                                return new VMachine(guid);
                            }, 'guid'
                        );
                        for (i = 0; i < self.vsas().length; i += 1) {
                            self.vsas()[i].load();
                        }
                        deferred.resolve();
                    })
                    .fail(deferred.reject)
                    .always(function() {
                        self.vsasLoading(false);
                    });
            }).promise();
        };


        // Durandal
        self.activate = function() {
            self.refresher.init(self.load, 5000);
            self.refresher.run();
            self.refresher.start();
            self.shared.footerData(self.vpools);
        };
        self.deactivate = function() {
            self.refresher.stop();
            self.shared.footerData(ko.observable());
        };
    };
});
