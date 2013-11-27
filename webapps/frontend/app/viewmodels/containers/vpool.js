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

        self.guid              = ko.observable(guid);
        self.name              = ko.observable();
        self.size              = ko.smoothObservable(0);
        self.iops              = ko.smoothDeltaObservable(0);
        self.storedData        = ko.smoothObservable(0);
        self.cache             = ko.smoothObservable();
        self.numberOfDisks     = ko.smoothObservable();
        self.numberOfMachines  = ko.smoothObservable();
        self.readSpeed         = ko.smoothDeltaObservable(2);
        self.writeSpeed        = ko.smoothDeltaObservable(2);
        self.backendWriteSpeed = ko.smoothDeltaObservable(2);
        self.backendReadSpeed  = ko.smoothDeltaObservable(2);
        self.backendType       = ko.observable();
        self.backendConnection = ko.observable();
        self.backendLogin      = ko.observable();

        self.freeSpace = ko.computed(function() {
            if (self.size() === 0 || self.storedData() === 0) {
                return 0;
            }
            return generic.round((self.size() - self.storedData()) / self.storedData() * 100, 2);
        });

        self.load = function() {
            self.loading(true);
            return $.Deferred(function(deferred) {
                $.when.apply($, [
                        $.Deferred(function(mainDeferred) {
                            generic.xhrAbort(self.loadHandle);
                            self.loadHandle = api.get('vpools/' + self.guid())
                                .done(function(data) {
                                    var type = '', stats = data.statistics,
                                        cache_hits = stats.sco_cache_hits + stats.cluster_cache_hits,
                                        cache_tries = cache_hits + stats.sco_cache_misses,
                                        cache_ratio = cache_hits / (cache_tries !== 0 ? cache_tries : 1) * 100;
                                    if (data.backend_type) {
                                        type = $.t('ovs:vpools.backendtypes.' + data.backend_type);
                                    }
                                    self.name(data.name);
                                    self.iops(stats.write_operations + stats.read_operations);
                                    self.size(data.size);
                                    self.storedData(data.stored_data);
                                    self.cache(cache_ratio);
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
                    .done(deferred.resolve)
                    .fail(deferred.reject)
                    .always(function() {
                        self.loading(false);
                    });
            }).promise();
        };
    };
});
