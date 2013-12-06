// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'knockout', 'ovs/generic'
], function(ko, generic) {
    "use strict";
    return function() {
        var self = this;

        self.text   = undefined;
        self.items  = ko.observableArray([]);
        self.target = ko.observable();
        self.set    = function(item) {
            self.target(item);
        };

        self.activate = function(settings) {
            if (!settings.hasOwnProperty('items')) {
                throw 'Items should be specified';
            }
            if (!settings.hasOwnProperty('target')) {
                throw 'Target should be specified';
            }
            self.items = settings.items;
            self.target = settings.target;
            if (self.target() === undefined && self.items().length > 0) {
                self.target(self.items()[0]);
            }
            self.text = generic.tryGet(settings, 'text', function(item) { return item; });
        };
    };
});
