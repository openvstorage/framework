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
        self.vsa          = ko.observable();
        self.vMachine     = ko.observable();
        self.vpool        = ko.observable();

        // Obserables
        self.loading      = ko.observable(false);
        self.loaded       = ko.observable(false);

        self.guid         = ko.observable(guid);
        self.name         = ko.observable();
        self.order        = ko.observable(0);
        self.snapshots    = ko.observableArray([]);
        self.size         = ko.smoothObservable(undefined, generic.formatBytes);
        self.storedData   = ko.smoothObservable(undefined, generic.formatBytes);
        self.cacheHits    = ko.smoothDeltaObservable();
        self.cacheMisses  = ko.smoothDeltaObservable();
        self.iops         = ko.smoothDeltaObservable(generic.formatNumber);
        self.readSpeed    = ko.smoothDeltaObservable(generic.formatSpeed);
        self.writeSpeed   = ko.smoothDeltaObservable(generic.formatSpeed);
        self.vsaGuid      = ko.observable();
        self.vpoolGuid    = ko.observable();
        self.vMachineGuid = ko.observable();
        self.failoverMode = ko.observable();

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
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };
        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                $.when.apply($, [
                        $.Deferred(function(deferred) {
                            generic.xhrAbort(self.loadHandle);
                            self.loadHandle = api.get('vdisks/' + self.guid())
                                .done(function(data) {
                                    var stats = data.statistics;
                                    self.name(data.name);
                                    if (stats !== undefined) {
                                        self.iops(stats.write_operations + stats.read_operations);
                                        self.cacheHits(stats.sco_cache_hits + stats.cluster_cache_hits);
                                        self.cacheMisses(stats.sco_cache_misses);
                                        self.readSpeed(stats.data_read);
                                        self.writeSpeed(stats.data_written);
                                    }
                                    self.order(data.order);
                                    self.snapshots(data.snapshots);
                                    self.size(data.size);
                                    self.storedData(data.info.stored);
                                    self.failoverMode(data.info.failover_mode);
                                    self.vpoolGuid(data.vpool_guid);
                                    self.vMachineGuid(data.vmachine_guid);
                                    deferred.resolve();
                                })
                                .fail(deferred.reject);
                        }).promise()
                    ])
                    .done(function() {
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
