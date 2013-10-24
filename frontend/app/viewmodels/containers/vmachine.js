define([
    'jquery', 'knockout',
    'ovs/generic', 'ovs/api',
    'viewmodels/containers/vdisk'
], function ($, ko, generic, api, VDisk) {
    "use strict";
    return function (guid) {
        var self = this;

        // Variables
        self.loadVDisksHandle = undefined;
        self.loadHandle       = undefined;

        // Obserables
        self.guid       = ko.observable(guid);
        self.name       = ko.observable();
        self.vDisks     = ko.observableArray([]);
        self.vDiskGuids = [];

        // Functions
        self.loadDisks = function () {
            return $.Deferred(function (deferred) {
                generic.xhrAbort(self.loadVDisksHandle);
                self.loadVDisksHandle = api.get('vdisks', undefined, {vmachineguid: self.guid()})
                    .done(function (data) {
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
        self.load = function () {
            return $.when.apply($, [
                self.loadDisks(),
                $.Deferred(function (deferred) {
                    generic.xhrAbort(self.loadHandle);
                    self.loadHandle = api.get('vmachines/' + self.guid())
                        .done(function (data) {
                            self.name(data.name);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                }).promise()
            ]);
        };
    };
});