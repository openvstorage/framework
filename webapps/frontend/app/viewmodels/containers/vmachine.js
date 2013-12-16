// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'jquery', 'knockout',
    'ovs/generic', 'ovs/api',
    'viewmodels/containers/vdisk'
], function($, ko, generic, api, VDisk) {
    "use strict";
    return function(guid) {
        var self = this;

        // Variables
        self.loadVDisksHandle = undefined;
        self.loadVSAGuid      = undefined;
        self.loadHandle       = undefined;
        self.loadVpoolGuid    = undefined;
        self.loadChildrenGuid = undefined;

        // External dependencies
        self.vsas           = ko.observableArray([]);
        self.vpools         = ko.observableArray([]);

        // Observables
        self.loading        = ko.observable(false);
        self.loaded         = ko.observable(false);

        self.guid           = ko.observable(guid);
        self.vpool          = ko.observable();
        self.vsaGuids       = ko.observableArray([]);
        self.vPoolGuids     = ko.observableArray([]);
        self.name           = ko.observable();
        self.ipAddress      = ko.observable();
        self.isInternal     = ko.observable();
        self.isVTemplate    = ko.observable();
        self.snapshots      = ko.observableArray([]);
        self.status         = ko.observable();
        self.iops           = ko.smoothDeltaObservable(generic.formatNumber);
        self.storedData     = ko.smoothObservable(undefined, generic.formatBytes);
        self.cacheHits      = ko.smoothDeltaObservable();
        self.cacheMisses    = ko.smoothDeltaObservable();
        self.readSpeed      = ko.smoothDeltaObservable(generic.formatSpeed);
        self.writeSpeed     = ko.smoothDeltaObservable(generic.formatSpeed);
        self.backendReads   = ko.smoothObservable(undefined, generic.formatNumber);
        self.backendWritten = ko.smoothObservable(undefined, generic.formatBytes);
        self.backendRead    = ko.smoothObservable(undefined, generic.formatBytes);
        self.bandwidthSaved = ko.smoothObservable(undefined, generic.formatBytes);
        self.failoverMode   = ko.observable();
        self.cacheRatio     = ko.computed(function() {
            var total = (self.cacheHits.raw() || 0) + (self.cacheMisses.raw() || 0);
            if (total === 0) {
                total = 1;
            }
            return generic.formatRatio((self.cacheHits.raw() || 0) / total * 100);
        });

        self.vDisks                = ko.observableArray([]);
        self.vDiskGuids            = [];
        self.templateChildrenGuids = ko.observableArray([]);

        self._bandwidth = ko.computed(function() {
            var total = (self.readSpeed.raw() || 0) + (self.writeSpeed.raw() || 0),
                initialized = self.readSpeed.initialized() && self.writeSpeed.initialized();
            return {
                value: generic.formatSpeed(total),
                initialized: initialized
            };
        });
        self.bandwidth = ko.computed(function() {
            return self._bandwidth().value;
        });
        self.bandwidth.initialized = ko.computed(function() {
            return self._bandwidth().initialized;
        });

        // Functions
        self.fetchVSAGuids = function() {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.loadVSAGuid);
                self.loadVSAGuid = api.get('vmachines/' + self.guid() + '/get_vsas')
                    .done(function(data) {
                        self.vsaGuids(data);
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };
        self.fetchVPoolGuids = function() {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.loadVpoolGuid);
                self.loadVpoolGuid = api.get('vmachines/' + self.guid() + '/get_vpools')
                    .done(function(data) {
                        self.vPoolGuids(data);
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };
        self.fetchTemplateChildrenGuids = function() {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.loadChildrenGuid);
                self.loadChildrenGuid = api.get('vmachines/' + self.guid() + '/get_children')
                    .done(function(data) {
                        self.templateChildrenGuids(data);
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };
        self.loadDisks = function() {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.loadVDisksHandle);
                self.loadVDisksHandle = api.get('vdisks', undefined, {vmachineguid: self.guid()})
                    .done(function(data) {
                        var i, guids = [];
                        for (i = 0; i < data.length; i += 1) {
                            guids.push(data[i].guid);
                        }
                        generic.crossFiller(
                            guids, self.vDiskGuids, self.vDisks,
                            function(guid) {
                                return new VDisk(guid);
                            }
                        );
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };
        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                $.when.apply($, [
                        self.loadDisks(),
                        $.Deferred(function(deferred) {
                            generic.xhrAbort(self.loadHandle);
                            self.loadHandle = api.get('vmachines/' + self.guid())
                                .done(function(data) {
                                    var stats = data.statistics;
                                    self.name(data.name);
                                    self.iops(stats.write_operations + stats.read_operations);
                                    self.storedData(data.stored_data);
                                    self.cacheHits(stats.sco_cache_hits + stats.cluster_cache_hits);
                                    self.cacheMisses(stats.sco_cache_misses);
                                    self.readSpeed(stats.data_read);
                                    self.writeSpeed(stats.data_written);
                                    self.backendWritten(stats.data_written);
                                    self.backendRead(stats.data_read);
                                    self.backendReads(stats.backend_read_operations);
                                    self.bandwidthSaved(stats.data_read - stats.backend_data_read);
                                    self.ipAddress(data.ip);
                                    self.isInternal(data.is_internal);
                                    self.isVTemplate(data.is_vtemplate);
                                    self.snapshots(data.snapshots);
                                    self.status(data.status.toLowerCase());
                                    self.failoverMode(data.failover_mode.toLowerCase());

                                    self.snapshots.sort(function(a, b) {
                                        // Newest first
                                        return b.timestamp - a.timestamp;
                                    });

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
