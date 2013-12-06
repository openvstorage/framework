// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'knockout', 'ovs/generic'
], function(ko, generic) {
    "use strict";
    return function() {
        var self = this;

        self.dataLoading     = ko.observable(false);
        self.widgetActivated = ko.observable(false);
        self.data            = ko.observable();
        self.hasData         = ko.computed(function() {
            return !(
                (!self.widgetActivated()) ||  // The widget is not loaded yet
                (!self.data()) ||             // The current observable is not set
                // The observed data is not set, or an empyt list
                (!self.data()() || (self.data()().hasOwnProperty('length') && self.data()().length === 0))
            );
        }).extend({ throttle: 50 });

        self._fetchData = function(observable, property) {
            self.dataLoading(true);
            var total = 0, i;
            if (observable instanceof Array) {
                for (i = 0; i < observable.length; i += 1) {
                    total += (observable[i][property].raw() || 0);
                }
            } else if (observable !== undefined) {
                total = observable[property].raw() || 0;
            }
            self.dataLoading(false);
            return total;
        };

        self.backendReads = ko.computed(function() {
            var total = 0;
            if (self.hasData()) {
                total = self._fetchData(self.data()(), 'backendReads');
            }
            return generic.formatNumber(total);
        });
        self.backendWritten = ko.computed(function() {
            var total = 0;
            if (self.hasData()) {
                total = self._fetchData(self.data()(), 'backendWritten');
            }
            return generic.formatBytes(total);
        });
        self.backendRead = ko.computed(function() {
            var total = 0;
            if (self.hasData()) {
                total = self._fetchData(self.data()(), 'backendRead');
            }
            return generic.formatBytes(total);
        });
        self.bandwidthSaved = ko.computed(function() {
            var total = 0;
            if (self.hasData()) {
                total = self._fetchData(self.data()(), 'bandwidthSaved');
            }
            return generic.formatBytes(total);
        });

        self.activate = function(settings) {
            if (!settings.hasOwnProperty('data')) {
                throw 'Data should be specified';
            }
            self.data = settings.data;
            self.widgetActivated(true);
        };
    };
});
