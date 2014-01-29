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
        self.viewportkeys    = [];
        self.key             = ko.observable();
        self.internalCurrent = ko.observable(1);
        self.headers         = ko.observableArray([]);
        self.settings        = ko.observable({});
        self.pagesize        = ko.observable(0);
        self.padding         = ko.observable(2);
        self.controls        = ko.observable(true);

        self.items = ko.computed(function() {
            var settings = self.settings();
            if (settings.hasOwnProperty('items')) {
                return self.settings().items();
            }
            return [];
        });
        self.showControls = ko.computed(function() {
            return self.controls() || (self.totalItems() > self.pagesize());
        });
        self.totalItems = ko.computed(function() {
            return self.items().length;
        });
        self.lastPage = ko.computed(function() {
            return Math.floor((self.totalItems() - 1) / self.pagesize()) + 1;
        });
        self.current = ko.computed({
            // One-based
            read: function() {
                return Math.min(self.internalCurrent(), Math.floor(self.totalItems() / Math.max(1, self.pagesize())) + 1);
            },
            write: function(value) {
                self.internalCurrent(value);
            }
        });
        self.hasNext = ko.computed(function() {
            return self.current() < self.lastPage();
        });
        self.hasPrevious = ko.computed(function() {
            return self.current() > 1;
        });
        self.pageFirst = ko.computed(function() {
            return (self.current() - 1) * self.pagesize() + 1;
        });
        self.pageLast = ko.computed(function() {
            return Math.min(self.pageFirst() + self.pagesize() - 1, self.items().length);
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
                vIndexes = [],
                start = (self.current() - 1) * self.pagesize(),
                max = Math.min(start + self.pagesize(), items.length);
            if (self.key() !== undefined) {
                for (i = start; i < max; i += 1) {
                    if (self.enterViewport !== undefined && $.inArray(items[i][self.key()](), self.viewportkeys) === -1) {
                        // If the viewport changes and an item enters the viewport, we'll request an update
                        self.enterViewport(items[i]);
                    }
                    vIndexes.push(items[i][self.key()]());
                    vItems.push(items[i]);
                }
            }
            self.viewportkeys = vIndexes.slice();
            return vItems;
        });

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
        self.viewportRefresh = function() {
            var i, items = self.viewportItems();
            for (i = 0; i < items.length; i += 1) {
                if (self.enterViewport !== undefined) {
                    // If there is viewport refreshing, we also request an update
                    self.enterViewport(items[i]);
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
            if (!settings.hasOwnProperty('key')) {
                throw 'Key should be specified';
            }

            self.enterViewport = generic.tryGet(settings, 'enterViewport');
            self.refresh = generic.tryGet(settings, 'viewportRefresh');
            self.initialLoad = generic.tryGet(settings, 'initialLoad', ko.observable(false));
            self.settings(settings);
            self.headers(settings.headers);
            self.pagesize(settings.pagesize);
            self.controls(generic.tryGet(settings, 'controls', true));
            self.key(settings.key);

            if (self.refresh !== undefined) {
                self.refresher.init(self.viewportRefresh, self.refresh);
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
