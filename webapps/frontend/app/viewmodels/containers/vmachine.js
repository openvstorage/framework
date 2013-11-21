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
        self.iops       = ko.deltaObservable();
        self.storedData = ko.smoothObservable();
        self.cache      = ko.smoothObservable();
        self.latency    = ko.smoothObservable();
        self.readSpeed  = ko.smoothObservable();
        self.writeSpeed = ko.smoothObservable();

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
                                    self.name(data.name);
                                    self.iops(data.iops);
                                    self.storedData(data.stored_data);
                                    self.cache(data.cache);
                                    self.latency(data.latency);
                                    self.readSpeed(data.read_speed);
                                    self.writeSpeed(data.write_speed);
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
