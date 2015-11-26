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
    'jquery', 'knockout',
    'ovs/generic'
], function($, ko, generic) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.loadedObservable = '';

        // Observables
        self.colspan     = ko.observable(0);
        self.displaymode = ko.observable('span');
        self.items       = ko.observableArray([]);

        // Functions
        self.isLoaded = function(observable) {
            return observable[self.loadedObservable]();
        };

        // Durandal
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
