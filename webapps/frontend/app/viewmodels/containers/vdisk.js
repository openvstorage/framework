// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'jquery', 'knockout',
    'ovs/generic', 'ovs/api'
], function($, ko, generic, api) {
    "use strict";
    return function(data) {
        var self = this;

        // Variables
        self.loadHandle = undefined;

        // Obserables
        self.guid      = ko.observable(data.guid);
        self.name      = ko.observable();
        self.order     = ko.observable(0);
        self.snapshots = ko.observableArray([]);
        self.size      = ko.observable(0);

        // Functions
        self.load = function() {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.loadHandle);
                self.loadHandle = api.get('vdisks/' + self.guid())
                    .done(function(data) {
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
