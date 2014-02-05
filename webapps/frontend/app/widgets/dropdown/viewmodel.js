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

        self.text          = undefined;
        self.key           = ko.observable();
        self.keyIsFunction = ko.observable(false);
        self.items         = ko.observableArray([]);
        self.target        = ko.observableArray([]);
        self.multi         = ko.observable(false);
        self.selected      = ko.computed(function() {
            var items = [], i;
            for (i = 0; i < self.items().length; i += 1) {
                if (self.contains(self.items()[i])) {
                    items.push(self.items()[i]);
                }
            }
            return items;
        });
        self.set    = function(item) {
            if (self.multi()) {
                if (self.contains(item)) {
                    self.target.remove(item);
                } else {
                    self.target.push(item);
                }
            } else {
                self.target(item);
            }
        };
        self.contains = function(item) {
            return $.inArray(item, self.target()) !== -1;
        };

        self.activate = function(settings) {
            if (!settings.hasOwnProperty('items')) {
                throw 'Items should be specified';
            }
            if (!settings.hasOwnProperty('target')) {
                throw 'Target should be specified';
            }
            if (!settings.hasOwnProperty('key')) {
                throw 'Key should be specified';
            }
            if (!settings.hasOwnProperty('keyisfunction')) {
                throw 'Keyisfunction should be specified';
            }
            self.key(settings.key);
            self.keyIsFunction(settings.keyisfunction);
            self.items = settings.items;
            self.target = settings.target;

            if (self.target.isObservableArray) {
                self.multi(true);
            } else if (self.target() === undefined && self.items().length > 0) {
                self.target(self.items()[0]);
            }
            self.text = generic.tryGet(settings, 'text', function(item) { return item; });
            self.target.valueHasMutated();
        };
    };
});
