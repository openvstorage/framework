// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'jquery', 'knockout',
    'ovs/generic'
], function($, ko, generic) {
    "use strict";
    return function() {
        var self = this;

        self.loadedObservable = '';
        self.items            = ko.observableArray([]);
        self.isLoaded         = function(observable) {
            return observable[self.loadedObservable]();
        };

        self.activate = function(settings) {
            if (!settings.hasOwnProperty('items')) {
                throw 'Items should be specified';
            }
            self.loadedObservable = generic.tryGet(settings, 'loadedObservable', 'initialized');
            self.items = settings.items;
        };
    };
});
