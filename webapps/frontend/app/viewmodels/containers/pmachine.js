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
        self.loadHandle = undefined;

        // Observables
        self.loading   = ko.observable(false);
        self.loaded    = ko.observable(false);

        self.guid      = ko.observable(guid);
        self.name      = ko.observable();
        self.ipAddress = ko.observable();
        self.hvtype    = ko.observable();

        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                api.get('pmachines/' + self.guid())
                    .done(function(data) {
                        self.name(data.name);
                        self.hvtype(data.hvtype);
                        self.ipAddress(data.ip);

                        self.loaded(true);
                        deferred.resolve();
                    })
                    .fail(deferred.reject)
                    .always(function() {
                        self.loading(false);
                    })
            }).promise();
        }
    };
});
