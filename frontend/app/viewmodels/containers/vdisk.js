define(['jquery', 'knockout', 'ovs/generic', 'ovs/authentication'], function ($, ko, generic, authentication) {
    "use strict";
    return function (data) {
        var self = this;

        // Variables
        self.load_handle = undefined;

        // Obserables
        self.guid = ko.observable(data.guid);
        self.name = ko.observable();
        self.order = ko.observable(0);
        self.snapshots = ko.observableArray([]);
        self.size = ko.observable(0);

        self.selected_snapshot = ko.observable();

        // Functions
        self.load = function () {
            return $.Deferred(function (deferred) {
                if (self.load_handle !== undefined) {
                    self.load_handle.abort();
                }
                self.load_handle = $.ajax('/api/internal/vdisks/' + self.guid() + '/?timestamp=' + generic.gettimestamp(), {
                    type: 'get',
                    contentType: 'application/json',
                    headers: {
                        'Authorization': authentication.header()
                    }
                })
                .done(function (data) {
                    self.name(data.name);
                    self.order(data.order);
                    self.snapshots(data.snapshots);
                    self.size(data.size);
                    deferred.resolve();
                })
                .fail(deferred.reject);
            }).promise();
        };
    };
});