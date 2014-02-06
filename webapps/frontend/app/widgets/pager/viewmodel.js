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
    'ovs/generic', 'ovs/refresher'
], function($, ko, generic, Refresher) {
    "use strict";
    return function() {
        var self = this;

        // System
        self.refresher       = new Refresher();

        // Fields
        self.internalCurrent = ko.observable(1);
        self.headers         = ko.observableArray([]);
        self.settings        = ko.observable({});
        self.padding         = ko.observable(2);
        self.controls        = ko.observable(true);
        self.preloadPage     = 0;

        self.items = ko.computed(function() {
            var settings = self.settings();
            if (settings.hasOwnProperty('items')) {
                return self.settings().items();
            }
            return [];
        });
        self.showControls = ko.computed(function() {
            return self.controls() || (self.totalItems() > 10);
        });
        self.totalItems = ko.computed(function() {
            return self.items().length;
        });
        self.lastPage = ko.computed(function() {
            return Math.floor((self.totalItems() - 1) / 10) + 1;
        });
        self.current = ko.computed({
            // One-based
            read: function() {
                return Math.min(self.internalCurrent(), Math.floor(self.totalItems() / 10) + 1);
            },
            write: function(value) {
                self.internalCurrent(value);
                self.viewportRefresh(value);
                // Prefetch/refresh surrounding pages
                if (value < self.lastPage()) {
                    self.viewportRefresh(value + 1);
                }
                if (value > 1) {
                    self.viewportRefresh(value - 1);
                }
            }
        });
        self.hasNext = ko.computed(function() {
            return self.current() < self.lastPage();
        });
        self.hasPrevious = ko.computed(function() {
            return self.current() > 1;
        });
        self.pageFirst = ko.computed(function() {
            return (self.current() - 1) * 10 + 1;
        });
        self.pageLast = ko.computed(function() {
            return Math.min(self.pageFirst() + 9, self.items().length);
        });
        self.pages = ko.computed(function() {
            var i,
                pages = [],
                from = Math.max(1, self.current() - self.padding()),
                to = Math.min(self.lastPage(), self.current() + self.padding());
            from = Math.max(1, Math.min(to - 2 * self.padding(), from));
            to = Math.min(self.lastPage(), Math.max(from + 2 * self.padding(), to));
            for (i = from; i <= to; i += 1) {
                pages.push(i);
            }
            return pages;
        });
        self.viewportItems = ko.computed(function() {
            var i,
                items = self.items(),
                vItems = [],
                start = (self.current() - 1) * 10,
                max = Math.min(start + 10, items.length);
            for (i = start; i < max; i += 1) {
                vItems.push(items[i]);
            }
            return vItems;
        }).extend({ throttle: 50 });

        self.step = function(next) {
            if (next) {
                if (self.hasNext()) {
                    self.current(self.current() + 1);
                }
            } else {
                if (self.hasPrevious()) {
                    self.current(self.current() - 1);
                }
            }
        };

        self.activate = function(settings) {
            if (!settings.hasOwnProperty('items')) {
                throw 'Items should be specified';
            }
            if (!settings.hasOwnProperty('headers')) {
                throw 'Headers should be specified';
            }

            self.refresh = generic.tryGet(settings, 'viewportRefreshInterval');
            self.viewportRefresh = generic.tryGet(settings, 'viewportRefresh');
            self.initialLoad = generic.tryGet(settings, 'initialLoad', ko.observable(false));
            self.settings(settings);
            self.headers(settings.headers);
            self.controls(generic.tryGet(settings, 'controls', true));

            if (self.refresh !== undefined) {
                self.refresher.init(function() {
                    self.viewportRefresh(self.current());
                    self.preloadPage += 1;
                    if (self.preloadPage === self.current()) {
                        self.preloadPage += 1;
                    }
                    if (self.preloadPage > self.lastPage()) {
                        self.preloadPage = 1;
                    }
                    if (self.preloadPage !== self.current()) {
                        self.viewportRefresh(self.preloadPage);
                    }
                }, self.refresh);
                self.refresher.start();
                settings.bindingContext.$root.widgets.push(self);
            }
        };
        self.deactivate = function() {
            if (self.refresh !== undefined) {
                self.refresher.stop();
            }
        };
    };
});
