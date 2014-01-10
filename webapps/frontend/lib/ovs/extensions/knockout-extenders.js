// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define(['knockout'], function(ko) {
    "use strict";
    ko.extenders.numeric = function(target, settings) {
        var computed;
        computed = ko.computed({
            read: target,
            write: function(newValue) {
                var parsedValue = parseInt(newValue, 10);
                if (isNaN(parsedValue)) {
                    parsedValue = 0;
                }
                if (settings.hasOwnProperty('min')) {
                    parsedValue = Math.max(settings.min, parsedValue);
                }
                target(parsedValue);
                target.notifySubscribers(parsedValue);
            }
        }).extend({ notify: 'always' });
        computed(target());
        return computed;
    };
});
