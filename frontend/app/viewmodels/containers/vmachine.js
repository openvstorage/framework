define(['jquery', 'knockout', 'ovs/generic', 'ovs/authentication', 'viewmodels/containers/vdisk'], function ($, ko, generic, authentication, VDisk) {
    "use strict";

    return function (data) {
        var self = this;
        // Variables
        self.load_vdisks_handle = undefined;
        self.load_handle = undefined;

        // Obserables
        self.guid = ko.observable(data.guid);
        self.name = ko.observable();
        self.vdisks = ko.observableArray([]);

        // Functions
        self.load_disks = function () {
            if (self.load_vdisks_handle !== undefined) {
                self.load_vdisks_handle.abort();
            }
            self.load_vdisks_handle = $.ajax('/api/internal/vdisks/?vmachine=' + self.guid() + '&timestamp=' + generic.gettimestamp(), {
                type: 'get',
                contentType: 'application/json',
                headers: {
                    'Authorization': authentication.header()
                }
            })
            .done(function (data) {
                var i;
                for (i = 0; i < data.length; i += 1) {
                    self.vdisks.push(new VDisk({guid: data[i]}));
                }
            });
        };
        self.load = function () {
            if (self.load_handle !== undefined) {
                self.load_handle.abort();
            }
            self.load_handle = $.ajax('/api/internal/vmachines/' + self.guid() + '/?timestamp=' + generic.gettimestamp(), {
                type: 'get',
                contentType: 'application/json',
                headers: {
                    'Authorization': authentication.header()
                }
            })
            .done(function (data) {
                self.name(data.name);
            })
        };
    };
});