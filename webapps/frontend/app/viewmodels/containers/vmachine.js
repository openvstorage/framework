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

        self.guid       = ko.observable(guid);
        self.name       = ko.observable();
        self.iops       = ko.smoothDeltaObservable(0);
        self.storedData = ko.smoothObservable();
        self.cache      = ko.smoothObservable();
        self.readSpeed  = ko.smoothDeltaObservable(2);
        self.writeSpeed = ko.smoothDeltaObservable(2);

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
                                    var cache_tries = data.cache_hits + data.cache_misses,
                                        cache_ratio = cache_tries / (data.cache_hits !== 0 ? data.cache_hits : 1) * 100;
                                    self.name(data.name);
                                    self.iops(data.write_operations + data.read_operations);
                                    self.storedData(data.stored_data);
                                    self.cache(cache_ratio);
                                    self.readSpeed(data.bytes_read);
                                    self.writeSpeed(data.bytes_written);
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
