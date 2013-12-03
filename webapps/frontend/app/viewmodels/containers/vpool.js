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
        self.loadHandle       = undefined;
        self.diskHandle       = undefined;
        self.machineHandle    = undefined;

        // Obserables
        self.loading           = ko.observable(false);
        self.loaded            = ko.observable(false);

        self.guid              = ko.observable(guid);
        self.name              = ko.observable();
        self.size              = ko.smoothObservable(undefined, generic.formatBytes);
        self.iops              = ko.smoothDeltaObservable(generic.formatNumber);
        self.storedData        = ko.smoothObservable(undefined, generic.formatBytes);
        self.cacheHits         = ko.smoothDeltaObservable();
        self.cacheMisses       = ko.smoothDeltaObservable();
        self.numberOfDisks     = ko.smoothObservable(undefined);
        self.numberOfMachines  = ko.smoothObservable(undefined);
        self.readSpeed         = ko.smoothDeltaObservable(generic.formatSpeed);
        self.writeSpeed        = ko.smoothDeltaObservable(generic.formatSpeed);
        self.backendWriteSpeed = ko.smoothDeltaObservable(generic.formatSpeed);
        self.backendReadSpeed  = ko.smoothDeltaObservable(generic.formatSpeed);
        self.backendType       = ko.observable();
        self.backendConnection = ko.observable();
        self.backendLogin      = ko.observable();

        self.cacheRatio = ko.computed(function() {
            var total = (self.cacheHits.raw() || 0) + (self.cacheMisses.raw() || 0);
            if (total === 0) {
                total = 1;
            }
            return generic.formatRatio((self.cacheHits.raw() || 0) / total * 100);
        });
        self.freeSpace = ko.computed(function() {
            if (self.size.raw() === 0 || self.storedData.raw() === 0) {
                return 0;
            }
            return generic.formatRatio((self.size.raw() - self.storedData.raw()) / self.size.raw() * 100);
        });

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

        self.load = function() {
            self.loading(true);
            return $.Deferred(function(deferred) {
                $.when.apply($, [
                        $.Deferred(function(mainDeferred) {
                            generic.xhrAbort(self.loadHandle);
                            self.loadHandle = api.get('vpools/' + self.guid())
                                .done(function(data) {
                                    var type = '', stats = data.statistics;
                                    if (data.backend_type) {
                                        type = $.t('ovs:vpools.backendtypes.' + data.backend_type);
                                    }
                                    self.name(data.name);
                                    self.iops(stats.write_operations + stats.read_operations);
                                    self.size(data.size);
                                    self.storedData(data.stored_data);
                                    self.cacheHits(stats.sco_cache_hits + stats.cluster_cache_hits);
                                    self.cacheMisses(stats.sco_cache_misses);
                                    self.readSpeed(stats.data_read);
                                    self.writeSpeed(stats.data_written);
                                    self.backendReadSpeed(stats.backend_data_read);
                                    self.backendWriteSpeed(stats.backend_data_written);
                                    self.backendType(type);
                                    self.backendConnection(data.backend_connection);
                                    self.backendLogin(data.backend_login);
                                    mainDeferred.resolve();
                                })
                                .fail(mainDeferred.reject);
                        }).promise(),
                        $.Deferred(function(diskDeferred) {
                            generic.xhrAbort(self.diskHandle);
                            self.diskHandle = api.get('vpools/' + self.guid() + '/count_disks')
                                .done(function(data) {
                                    self.numberOfDisks(data);
                                    diskDeferred.resolve();
                                })
                                .fail(diskDeferred.reject);
                        }).promise(),
                        $.Deferred(function(machineDeferred) {
                            generic.xhrAbort(self.machineHandle);
                            self.machineHandle = api.get('vpools/' + self.guid() + '/count_machines')
                                .done(function(data) {
                                    self.numberOfMachines(data);
                                    machineDeferred.resolve();
                                })
                                .fail(machineDeferred.reject);
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
