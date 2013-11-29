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
        self.item             = ko.observable();
        self.isLazyLoader     = ko.observable(false);
        self.isLoaded         = ko.computed(function() {
            if (self.isLazyLoader()) {
                return self.item[self.loadedObservable]();
            }
            return true;
        });

        self.activate = function(settings) {
            if (!settings.hasOwnProperty('item')) {
                throw 'Item should be specified';
            }
            self.loadedObservable = generic.tryGet(settings, 'loadedObservable', 'initialized');
            self.item = settings.item;
            if (self.item.hasOwnProperty(self.loadedObservable) && self.item[self.loadedObservable].call) {
                self.isLazyLoader(true);
            }
        };
    };
});
