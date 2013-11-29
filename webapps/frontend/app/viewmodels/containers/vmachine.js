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
        self.loadHandle       = undefined;

        // Obserables
        self.loading    = ko.observable(false);

        self.guid        = ko.observable(guid);
        self.name        = ko.observable();
        self.snapshots   = ko.observable();
        self.iops        = ko.smoothDeltaObservable(generic.formatShort);
        self.storedData  = ko.smoothObservable(undefined, generic.formatBytes);
        self.cacheHits   = ko.smoothDeltaObservable();
        self.cacheMisses = ko.smoothDeltaObservable();
        self.readSpeed   = ko.smoothDeltaObservable(generic.formatSpeed);
        self.writeSpeed  = ko.smoothDeltaObservable(generic.formatSpeed);
        self.cacheRatio  = ko.computed(function() {
            var total = (self.cacheHits.raw() || 0) + (self.cacheMisses.raw() || 0);
            if (total === 0) {
                total = 1;
            }
            return generic.formatRatio((self.cacheHits.raw() || 0) / total * 100);
        });

        self.vDisks     = ko.observableArray([]);
        self.vDiskGuids = [];

        // Functions
        self.loadDisks = function() {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.loadVDisksHandle);
                self.loadVDisksHandle = api.get('vdisks', undefined, {vmachineguid: self.guid()})
                    .done(function(data) {
                        var i, item;
                        for (i = 0; i < data.length; i += 1) {
                            item = data[i];
                            if ($.inArray(item.guid, self.vDiskGuids) === -1) {
                                self.vDiskGuids.push(item.guid);
                                self.vDisks.push(new VDisk(item));
                            }
                        }
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
                                    self.snapshots(data.snapshots);
                                    deferred.resolve();
                                })
                                .fail(deferred.reject);
                        }).promise()
                    ])
                    .done(deferred.resolve)
                    .fail(deferred.reject)
                    .always(function() {
                        self.loading(false);
                    });
            }).promise();
        };
    };
});
