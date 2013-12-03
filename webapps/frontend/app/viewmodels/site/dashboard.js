// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/api', 'ovs/refresher',
    '../containers/vmachine', '../containers/vpool'
], function(ko, shared, generic, api, Refresher, VMachine, VPool) {
    "use strict";
    return function() {
        var self = this;

        // System
        self.shared    = shared;
        self.guard     = { authenticated: true };
        self.refresher = new Refresher();

        self.loadVsasHandle   = undefined;
        self.loadVPoolsHandle = undefined;

        self.vsasLoading       = ko.observable(false);
        self.vPoolsLoading     = ko.observable(false);

        self.vsaGuids   = ko.observableArray([]);
        self.vsas       = ko.observableArray([]);
        self.vpoolGuids = ko.observableArray([]);
        self.vpools     = ko.observableArray([]);

        self._cacheRatio = ko.computed(function() {
            var hits = 0, misses = 0, total, initialized = true, i;
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
            return {
                value: generic.formatRatio(hits / total * 100),
                initialized: initialized
            };
        });
        self.cacheRatio = ko.computed(function() {
            return self._cacheRatio().value;
        });
        self.cacheRatio.initialized = ko.computed(function() {
            return self._cacheRatio().initialized;
        });
        self._iops = ko.computed(function() {
            var total = 0, initialized = true, i;
            for (i = 0; i < self.vpools().length; i += 1) {
                initialized = initialized && self.vpools()[i].iops.initialized();
                total += (self.vpools()[i].iops.raw() || 0);
            }
            return {
                value: generic.formatShort(total),
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

        // Functions
        self.load = function() {
            return $.Deferred(function(deferred) {
                $.when.apply($, [
                        self.loadVsas(),
                        self.loadVPools()
                    ])
                    .done(deferred.resolve)
                    .fail(deferred.reject);
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
                            }
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
                            }
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
        };
        self.deactivate = function() {
            self.refresher.stop();
        };
    };
});
