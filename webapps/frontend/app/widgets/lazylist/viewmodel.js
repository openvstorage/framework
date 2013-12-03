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
        self.colspan          = ko.observable(0);
        self.displaymode      = ko.observable('span');
        self.items            = ko.observableArray([]);
        self.isLoaded         = function(observable) {
            return observable[self.loadedObservable]();
        };

        self.activate = function(settings) {
            if (!settings.hasOwnProperty('items')) {
                throw 'Items should be specified';
            }
            self.displaymode(generic.tryGet(settings, 'displaymode', 'span'));
            self.colspan(generic.tryGet(settings, 'colspan', 0));
            self.loadedObservable = generic.tryGet(settings, 'loadedObservable', 'initialized');
            self.items = settings.items;
        };
    };
});
