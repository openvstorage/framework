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
/*global define, window */
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
        self.key            = ko.observable();
        self.keyIsFunction  = ko.observable(false);
        self.small          = ko.observable(false);
        self.multi          = ko.observable(false);
        self.free           = ko.observable(false);
        self.items          = ko.observableArray([]);
        self.target         = ko.observableArray([]);
        self._freeValue     = ko.observable();
        self.useFree        = ko.observable(false);
        self.emptyIsLoading = ko.observable(true);
        self.enabled        = ko.observable(true);

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
        self.extract = generic.extract;
        self.set = function(item) {
            if (self.multi()) {
                if (self.contains(item)) {
                    self.remove(item);
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
        self.remove = function(item) {
            if (self.key() === undefined) {
                return self.target.remove(item);
            }
            var itemIndex = -1;
            $.each(self.target(), function(index, targetItem) {
                if (self.keyIsFunction()) {
                    if (item[self.key()]() === targetItem[self.key()]()) {
                        itemIndex = index;
                        return false;
                    }
                } else if (item[self.key()] === targetItem[self.key()]) {
                    itemIndex = index;
                    return false;
                }
                return true;
            });
            self.target.splice(itemIndex, 1);
        };
        self.contains = function(item) {
            if (self.multi()) {
                if (self.key() === undefined) {
                    return self.target().contains(item);
                }
                var result = false, found;
                $.each(self.target(), function (index, targetItem) {
                    if (self.keyIsFunction()) {
                        found = item[self.key()]() === targetItem[self.key()]();
                        result |= found;
                        return !found;
                    } else {
                        found = item[self.key()] === targetItem[self.key()];
                        result |= found;
                        return !found;
                    }
                });
                return result;
            }
            return false;
        };
        self.translate = function() {
            window.setTimeout(function() { $('html').i18n(); }, 250);
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
            self.enabled = generic.tryGet(settings, 'enabled', ko.observable(true));
            self.key(generic.tryGet(settings, 'key', undefined));
            self.small(generic.tryGet(settings, 'small', false));
            self.keyIsFunction(generic.tryGet(settings, 'keyisfunction', false));
            self.free(generic.tryGet(settings, 'free', false));
            self.emptyIsLoading(generic.tryGet(settings, 'emptyisloading', true));
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
