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
        self._item            = ko.observable();
        self.undefinedLoading = ko.observable(true);
        self.isLoaded         = ko.computed(function() {
            var observable = self._item();
            if (observable === undefined) {
                return false;
            }
            if (observable.hasOwnProperty(self.loadedObservable)) {
                return observable[self.loadedObservable]();
            }
            if (!observable.call || observable() === undefined) {
                return false;
            }
            if (observable().hasOwnProperty(self.loadedObservable)) {
                return observable()[self.loadedObservable]();
            }
            return true;
        });
        self.item             = ko.computed(function() {
            var returnValue = self._item();
            if (returnValue !== undefined) {
                return returnValue();
            }
            return returnValue;
        });

        self.activate = function(settings) {
            if (!settings.hasOwnProperty('item')) {
                throw 'Item should be specified';
            }
            self.undefinedLoading(generic.tryGet(settings, 'undefinedLoading', true));
            self.loadedObservable = generic.tryGet(settings, 'loadedObservable', 'initialized');
            self._item(settings.item);
        };
    };
});
