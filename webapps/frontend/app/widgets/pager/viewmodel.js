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

        // Variables
        self.refresher   = new Refresher();
        self.preloadPage = 0;
        self.key         = undefined;

        // Observables
        self.items           = ko.observableArray([]);
        self.internalCurrent = ko.observable(1);
        self.totalItems      = ko.observable(0);
        self.lastPage        = ko.observable(1);
        self.pageFirst       = ko.observable(0);
        self.pageLast        = ko.observable(0);
        self.headers         = ko.observableArray([]);
        self.padding         = ko.observable(2);
        self.controls        = ko.observable(true);
        self.viewportKeys    = ko.observableArray([]);
        self.viewportItems   = ko.observableArray([]);
        self.pageLoading     = ko.observable(false);

        // Handles
        self.preloadHandle = {};
        self.loadHandle    = {};

        // Computed
        self.showControls = ko.computed(function() {
            return self.controls() || (self.totalItems() > 10);
        });
        self.current = ko.computed({
            // One-based
            read: function() {
                return self.internalCurrent();
            },
            write: function(value) {
                self.internalCurrent(value);
                self.load(value, false);
                // Prefetch/refresh surrounding pages
                if (value < self.lastPage()) {
                    self.load(value + 1, true);
                }
                if (value > 1) {
                    self.load(value - 1, true);
                }
            }
        });
        self.hasNext = ko.computed(function() {
            return self.current() < self.lastPage();
        });
        self.hasPrevious = ko.computed(function() {
            return self.current() > 1;
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
        self.viewportCalculator = ko.computed(function() {
            generic.crossFiller(
                self.viewportKeys(), self.viewportItems,
                function(key) {
                    var i;
                    for (i = 0; i < self.items().length; i += 1) {
                        if (self.items()[i][self.key]() === key) {
                            return self.items()[i];
                        }
                    }
                }, self.key
            );
        });
        self.loading = ko.computed(function() {
            return self.viewportItems().length === 0 && self.pageLoading();
        });

        // Functions
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
        self.load = function(page, preload) {
            if ((preload === true && self.preloadHandle[page] !== undefined && self.preloadHandle[page].state() === 'pending') ||
                (preload !== true && self.loadHandle[page] !== undefined && self.loadHandle[page].state() === 'pending')) {
                return;
            }
            self.pageLoading(true);
            $.each(self.viewportItems(), function(index, item) {
                item.loading(true);
            });
            var promise = self.loadData(page)
                .then(function(dataset) {
                    if (dataset !== undefined) {
                        self.totalItems(dataset.data._paging.total_items);
                        self.lastPage(dataset.data._paging.max_page);
                        self.pageFirst(dataset.data._paging.start_number);
                        self.pageLast(dataset.data._paging.end_number);
                        var keys = [], idata = {};
                        $.each(dataset.data.data, function(index, item) {
                            keys.push(item[self.key]);
                            idata[item[self.key]] = item;
                        });
                        self.viewportKeys(keys);
                        generic.crossFiller(keys, self.items, dataset.loader, self.key, false);
                        $.each(self.items(), function(index, item) {
                            if ($.inArray(item[self.key](), keys) !== -1) {
                                item.fillData(idata[item[self.key]()]);
                                item.loading(false);
                                if (dataset.dependencyLoader !== undefined) {
                                    dataset.dependencyLoader(item);
                                }
                            }
                        });
                    }
                    self.pageLoading(false);
                });
            if (preload) {
                self.preloadHandle[page] = promise;
            } else {
                self.loadHandle[page] = promise;
            }
        };

        // Durandal
        self.activate = function(settings) {
            if (!settings.hasOwnProperty('headers')) {
                throw 'Headers should be specified';
            }

            self.loadData = generic.tryGet(settings, 'loadData');
            self.refresh = parseInt(generic.tryGet(settings, 'refreshInterval', '5000'), 10);
            self.key = generic.tryGet(settings, 'key', 'guid');
            self.headers(settings.headers);
            self.controls(generic.tryGet(settings, 'controls', true));

            self.refresher.init(function() {
                self.load(self.current(), false);
                self.preloadPage += 1;
                if (self.preloadPage === self.current()) {
                    self.preloadPage += 1;
                }
                if (self.preloadPage > self.lastPage()) {
                    self.preloadPage = 1;
                }
                if (self.preloadPage !== self.current()) {
                    self.preloadHandle = self.load(self.preloadPage, true);
                }
            }, self.refresh);
            self.refresher.run();
            self.refresher.start();
            settings.bindingContext.$root.widgets.push(self);
        };
        self.deactivate = function() {
            self.refresher.stop();
        };
    };
});
