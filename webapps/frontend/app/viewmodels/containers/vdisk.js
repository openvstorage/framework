// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'jquery', 'knockout',
    'ovs/generic', 'ovs/api'
], function($, ko, generic, api) {
    "use strict";
    return function(guid) {
        var self = this;

        // Variables
        self.loadHandle             = undefined;
        self.loadVSAGuidHandle      = undefined;
        self.loadVMachineGuidHandle = undefined;

        // External dependencies
        self.vsa            = ko.observable();
        self.vMachine       = ko.observable();
        self.vpool          = ko.observable();

        // Observables
        self.loading        = ko.observable(false);
        self.loaded         = ko.observable(false);

        self.guid           = ko.observable(guid);
        self.name           = ko.observable();
        self.order          = ko.observable(0);
        self.snapshots      = ko.observableArray([]);
        self.size           = ko.smoothObservable(undefined, generic.formatBytes);
        self.storedData     = ko.smoothObservable(undefined, generic.formatBytes);
        self.cacheHits      = ko.smoothDeltaObservable();
        self.cacheMisses    = ko.smoothDeltaObservable();
        self.iops           = ko.smoothDeltaObservable(generic.formatNumber);
        self.readSpeed      = ko.smoothDeltaObservable(generic.formatSpeed);
        self.writeSpeed     = ko.smoothDeltaObservable(generic.formatSpeed);
        self.backendReads   = ko.smoothObservable(undefined, generic.formatNumber);
        self.backendWritten = ko.smoothObservable(undefined, generic.formatBytes);
        self.backendRead    = ko.smoothObservable(undefined, generic.formatBytes);
        self.bandwidthSaved = ko.smoothObservable(undefined, generic.formatBytes);
        self.vsaGuid        = ko.observable();
        self.vpoolGuid      = ko.observable();
        self.vMachineGuid   = ko.observable();
        self.failoverMode   = ko.observable();

        self.cacheRatio = ko.computed(function() {
            var total = (self.cacheHits.raw() || 0) + (self.cacheMisses.raw() || 0);
            if (total === 0) {
                total = 1;
            }
            return generic.formatRatio((self.cacheHits.raw() || 0) / total * 100);
        });

        // Functions
        self.fetchVSAGuid = function() {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.loadVSAGuid);
                self.loadVSAGuid = api.get('vdisks/' + self.guid() + '/get_vsa')
                    .done(function(data) {
                        self.vsaGuid(data);
                    })
                    .always(deferred.resolve);
            }).promise();
        };
        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                generic.xhrAbort(self.loadHandle);
                self.loadHandle = api.get('vdisks/' + self.guid())
                    .done(function(data) {
                        var stats = data.statistics,
                            statsTime = Math.round(stats.timestamp * 1000);
                        self.name(data.name);
                        self.iops({ value: stats.write_operations + stats.read_operations, timestamp: statsTime });
                        self.cacheHits({ value: stats.sco_cache_hits + stats.cluster_cache_hits, timestamp: statsTime });
                        self.cacheMisses({ value: stats.sco_cache_misses, timestamp: statsTime });
                        self.readSpeed({ value: stats.data_read, timestamp: statsTime });
                        self.writeSpeed({ value: stats.data_written, timestamp: statsTime });
                        self.backendWritten(stats.data_written);
                        self.backendRead(stats.data_read);
                        self.backendReads(stats.backend_read_operations);
                        self.bandwidthSaved(stats.data_read - stats.backend_data_read);
                        self.order(data.order);
                        self.snapshots(data.snapshots);
                        self.size(data.size);
                        self.storedData(data.info.stored);
                        self.failoverMode(data.info.failover_mode.toLowerCase() || 'unknown');
                        self.vpoolGuid(data.vpool_guid);
                        self.vMachineGuid(data.vmachine_guid);

                        self.snapshots.sort(function(a, b) {
                            // Sorting based on newest first
                            return b.timestamp - a.timestamp;
                        });

                        self.loaded(true);
                        deferred.resolve();
                    })
                    .fail(deferred.reject)
                    .always(function() {
                        self.loading(false);
                    });
            }).promise();
        };
    };
});
