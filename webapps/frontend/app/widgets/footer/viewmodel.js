// Copyright 2014 iNuron NV
//
// Licensed under the Open vStorage Modified Apache License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define([
    'knockout', 'ovs/generic', 'ovs/shared'
], function(ko, generic, shared) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared = shared;

        // Observables
        self.dataLoading     = ko.observable(false);
        self.widgetActivated = ko.observable(false);
        self.data            = ko.observable();

        // Computed
        self.hasData         = ko.computed(function() {
            return !(
                (!self.widgetActivated()) ||  // The widget is not loaded yet
                (!self.data()) ||             // The current observable is not set
                // The observed data is not set, or an empyt list
                (!self.data()() || (self.data()().hasOwnProperty('length') && self.data()().length === 0))
            );
        }).extend({ rateLimit: 50 });
        self.cacheHits = ko.computed(function() {
            var total = 0;
            if (self.hasData()) {
                total = self._fetchData(self.data()(), 'cacheHits');
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
        self.unregistered = ko.computed(function() {
            return shared.registration().registered === false && shared.registration().remaining > 0;
        });

        // Functions
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

        // Durandal
        self.activate = function(settings) {
            if (!settings.hasOwnProperty('data')) {
                throw 'Data should be specified';
            }
            self.data = settings.data;
            self.widgetActivated(true);
        };
    };
});
