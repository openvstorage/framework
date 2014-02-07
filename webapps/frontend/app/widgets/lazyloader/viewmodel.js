// Copyright 2014 CloudFounders NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

        // Observable
        self._item            = ko.observable();
        self.undefinedLoading = ko.observable(true);

        // Computed
        self.isLoaded = ko.computed(function() {
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
        self.item = ko.computed(function() {
            var returnValue = self._item();
            if (returnValue !== undefined) {
                return returnValue();
            }
            return returnValue;
        });

        // Durandal
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
