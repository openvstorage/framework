// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'jquery', 'knockout'
], function($, ko) {
    "use strict";
    return function() {
        var self = this;

        self.item = undefined;
        self.isLazyLoader = ko.observable(false);

        self.activate = function(settings) {
            if (!settings.hasOwnProperty('item')) {
                throw 'Item should be specified';
            }
            self.item = settings.item;
            if (self.item.hasOwnProperty('initialized') && self.item.initialized.call) {
                self.isLazyLoader(true);
            }
        };
    };
});
