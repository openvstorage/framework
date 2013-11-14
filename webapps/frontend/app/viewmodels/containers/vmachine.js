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
        self.initial          = true;

        // Obserables
        self.loading    = ko.observable(false);

        self.guid       = ko.observable(guid);
        self.name       = ko.observable();
        self.iops       = ko.observable();
        self.storedData = ko.observable();
        self.cache      = ko.observable();
        self.latency    = ko.observable();
        self.readSpeed  = ko.observable();
        self.writeSpeed = ko.observable();

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
                                    if (self.initial) {
                                        self.initial = false;
                                        self.iops(data.iops);
                                        self.storedData(data.stored_data);
                                        self.cache(data.cache);
                                        self.latency(data.latency);
                                        self.readSpeed(data.read_speed);
                                        self.writeSpeed(data.write_speed);
                                    } else {
                                        generic.smooth(self.iops, data.iops);
                                        generic.smooth(self.storedData, data.stored_data);
                                        generic.smooth(self.cache, data.cache);
                                        generic.smooth(self.latency, data.latency);
                                        generic.smooth(self.readSpeed, data.read_speed);
                                        generic.smooth(self.writeSpeed, data.write_speed);
                                    }
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