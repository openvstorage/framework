define(['jquery', 'knockout', 'ovs/generic', 'ovs/authentication', 'ovs/api', 'viewmodels/containers/vdisk'], function ($, ko, generic, authentication, api, VDisk) {
    "use strict";
    return function (guid) {
        var self = this;

        // Variables
        self.load_vdisks_handle = undefined;
        self.load_handle = undefined;

        // Obserables
        self.guid = ko.observable(guid);
        self.name = ko.observable();
        self.vdisks = ko.observableArray([]);
        self.vdisk_guids = [];

        // Functions
        self.load_disks = function () {
            return $.Deferred(function (deferred) {
                if (self.load_vdisks_handle !== undefined) {
                    self.load_vdisks_handle.abort();
                }
                self.load_vdisks_handle = api.get('vdisks', undefined, {vmachineguid: self.guid()})
                .done(function (data) {
                    var i, item;
                    for (i = 0; i < data.length; i += 1) {
                        item = data[i];
                        if ($.inArray(item.guid, self.vdisk_guids) === -1) {
                            self.vdisk_guids.push(item.guid);
                            self.vdisks.push(new VDisk(item));
                        }
                    }
                    deferred.resolve();
                })
                .fail(deferred.reject);
            }).promise();
        };
        self.load = function () {
            return $.when.apply($, [
                self.load_disks(),
                $.Deferred(function (deferred) {
                    if (self.load_handle !== undefined) {
                        self.load_handle.abort();
                    }
                    self.load_handle = api.get('vmachines/' + self.guid())
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