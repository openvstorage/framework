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
    'knockout', 'jquery',
    'ovs/generic'
], function(ko, $, generic) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.text          = undefined;
        self.unique        = generic.getTimestamp().toString();

        // Observables
        self.key           = ko.observable();
        self.keyIsFunction = ko.observable(false);
        self.small         = ko.observable(false);
        self.multi         = ko.observable(false);
        self.free          = ko.observable(false);
        self.items         = ko.observableArray([]);
        self.target        = ko.observableArray([]);
        self._freeValue    = ko.observable();
        self.useFree       = ko.observable(false);

        // Computed
        self.selected  = ko.computed(function() {
            var items = [], i;
            for (i = 0; i < self.items().length; i += 1) {
                if (self.contains(self.items()[i])) {
                    items.push(self.items()[i]);
                }
            }
            return items;
        });
        self.freeValue = ko.computed({
            read: function() {
                return self._freeValue();
            },
            write: function(newValue) {
                self.target(newValue);
                self._freeValue(newValue);
            }
        });

        // Functions
        self.set = function(item) {
            if (self.multi()) {
                if (self.contains(item)) {
                    self.target.remove(item);
                } else {
                    self.target.push(item);
                }
            } else {
                self.target(item);
                if (self.free() && $.inArray(self.target(), self.items()) === -1 && self.useFree()) {
                    self._freeValue(item);
                }
            }
        };
        self.contains = function(item) {
            return $.inArray(item, self.target()) !== -1;
        };

        // Durandal
        self.activate = function(settings) {
            if (!settings.hasOwnProperty('items')) {
                throw 'Items should be specified';
            }
            if (!settings.hasOwnProperty('target')) {
                throw 'Target should be specified';
            }
            self.items = settings.items;
            self.target = settings.target;
            self.key(generic.tryGet(settings, 'key', undefined));
            self.small(generic.tryGet(settings, 'small', false));
            self.keyIsFunction(generic.tryGet(settings, 'keyisfunction', false));
            self.free(generic.tryGet(settings, 'free', false));
            if (self.free()) {
                if (!settings.hasOwnProperty('defaultfree')) {
                    throw 'If free values are allowed, a default should be provided';
                }
                if (self.target() !== undefined) {
                    self._freeValue(self.target());
                } else {
                    self._freeValue(settings.defaultfree);
                }
            }
            self.text = generic.tryGet(settings, 'text', function(item) { return item; });
            if (self.target.isObservableArray) {
                self.multi(true);
            } else if (self.target() === undefined && self.items().length > 0) {
                var foundDefault = false;
                $.each(self.items(), function(index, item) {
                    if (settings.hasOwnProperty('defaultRegex')) {
                        if (self.text(item).match(settings.defaultRegex) !== null) {
                            self.target(item);
                            foundDefault = true;
                            return false;
                        }
                        return true;
                    }
                    if (item === undefined) {
                        foundDefault = true;
                    }
                });
                if (!foundDefault) {
                    self.target(self.items()[0]);
                }
            }
            if (self.free() && self.multi()) {
                throw 'A dropdown cannot be a multiselect and allow free values at the same time.';
            }
            self.useFree(false);

            if (!ko.isComputed(self.target)) {
                self.target.valueHasMutated();
            }
        };
    };
});
