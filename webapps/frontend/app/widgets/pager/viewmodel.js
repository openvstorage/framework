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
        self.refresher     = new Refresher();
        self.preloadPage   = 0;
        self.key           = undefined;
        self.viewportCache = {};

        // Observables
        self.container       = ko.observable(ko.observableArray([]));
        self.external        = ko.observable(false);
        self.internalCurrent = ko.observable(1);
        self._totalItems     = ko.observable(0);
        self._lastPage       = ko.observable(1);
        self._pageFirst      = ko.observable(0);
        self._pageLast       = ko.observable(0);
        self.headers         = ko.observableArray([]);
        self.settings        = ko.observable({});
        self.padding         = ko.observable(2);
        self.controls        = ko.observable(true);
        self.viewportKeys    = ko.observableArray([]);
        self.viewportItems   = ko.observableArray([]);
        self.pageLoading     = ko.observable(false);

        // Handles
        self.preloadHandle = undefined;
        self.loadHandle    = undefined;

        // Computed
        self.items = ko.computed({
            read: function() {
                if (self.external()) {
                    if (self.settings().hasOwnProperty('items')) {
                        return self.settings().items();
                    }
                    return [];
                }
                return self.container()();
            },
            write: function(value) {
                self.container()(value);
            }
        });
        self.showControls = ko.computed(function() {
            return self.controls() || (self.totalItems() > 10);
        });
        self.totalItems = ko.computed(function() {
            if (self.external()) {
                return self.items().length;
            }
            return self._totalItems();
        });
        self.lastPage = ko.computed(function() {
            if (self.external()) {
                return Math.floor((self.totalItems() - 1) / 10) + 1;
            }
            return self._lastPage();
        });
        self.current = ko.computed({
            // One-based
            read: function() {
                if (self.external()) {
                    return Math.min(self.internalCurrent(), Math.floor(self.totalItems() / 10) + 1);
                }
                return self.internalCurrent();
            },
            write: function(value) {
                self.internalCurrent(value);
                self.load(value, false);

                // Prefetch/refresh next page
                var preloadPage = value + 1;
                if (preloadPage === self.lastPage()) {
                    preloadPage = 1;
                }
                self.hasLoad(true, true);
                self.load(preloadPage, true);
            }
        });
        self.hasNext = ko.computed(function() {
            return self.current() < self.lastPage();
        });
        self.hasPrevious = ko.computed(function() {
            return self.current() > 1;
        });
        self.pageFirst = ko.computed(function() {
            if (self.external()) {
                return Math.min((self.current() - 1) * 10 + 1, self.items().length);
            }
            return self._pageFirst();
        });
        self.pageLast = ko.computed(function() {
            if (self.external()) {
                return Math.min(self.pageFirst() + 9, self.items().length);
            }
            return self._pageLast();
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
            if (self.external()) {
                var i, items = self.items(), vItems = [],
                    start = (self.current() - 1) * 10,
                    max = Math.min(start + 10, items.length);
                for (i = start; i < max; i += 1) {
                    vItems.push(items[i]);
                }
                self.viewportItems(vItems);
            } else {
                self.container().sort(function (a, b) {
                    var aLocation = $.inArray(a[self.key](), self.viewportKeys()),
                        bLocation = $.inArray(b[self.key](), self.viewportKeys());
                    if (aLocation === -1) {
                        return 1;
                    }
                    if (bLocation === -1) {
                        return -1;
                    }
                    return aLocation - bLocation;
                });
                self.viewportItems(self.container()().slice(0, self.viewportKeys().length));
            }
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
        self.hasLoad = function(preload, abort) {
            if (preload) {
                if (self.preloadHandle !== undefined && self.preloadHandle.state() === 'pending') {
                    if (abort) {
                        generic.xhrAbort(self.preloadHandle);
                        return false;
                    }
                    return true;
                }
                return false;
            }
            if (self.loadHandle !== undefined && self.loadHandle.state() === 'pending') {
                if (abort) {
                    generic.xhrAbort(self.loadHandle);
                    return false;
                }
                return true;
            }
            return false;
        };
        self.load = function(page, preload) {
            if (self.external()) {
                self.loadData(page);
            } else {
                if (self.hasLoad(preload, !preload)) {
                    // If there's a preload, it shouldn't abort. If it's not a preload, it should abort.
                    return;
                }
                self.pageLoading(true);
                $.each(self.viewportItems(), function (index, item) {
                    item.loading(true);
                });
                if (!preload && self.viewportCache.hasOwnProperty(page)) {
                    self.viewportKeys(self.viewportCache[page].keys);
                    self._totalItems(self.viewportCache[page].totalItems);
                    self._lastPage(self.viewportCache[page].lastPage);
                    self._pageFirst(self.viewportCache[page].pageFirst);
                    self._pageLast(self.viewportCache[page].pageLast);
                }
                var promise = self.loadData(page)
                    .then(function (dataset) {
                        if (dataset !== undefined && (preload || page === self.current())) {
                            var keys = [], idata = {};
                            $.each(dataset.data.data, function (index, item) {
                                keys.push(item[self.key]);
                                idata[item[self.key]] = item;
                            });
                            self.viewportCache[page] = {
                                keys: keys,
                                totalItems: dataset.data._paging.total_items,
                                lastPage: dataset.data._paging.max_page,
                                pageFirst: dataset.data._paging.start_number,
                                pageLast: dataset.data._paging.end_number
                            };
                            if (!preload) {
                                self._totalItems(self.viewportCache[page].totalItems);
                                self._lastPage(self.viewportCache[page].lastPage);
                                self._pageFirst(self.viewportCache[page].pageFirst);
                                self._pageLast(self.viewportCache[page].pageLast);
                                self.viewportKeys(self.viewportCache[page].keys);
                            }
                            generic.crossFiller(keys, self.container(), dataset.loader, self.key, false);
                            $.each(self.container()(), function (index, item) {
                                if ($.inArray(item[self.key](), keys) !== -1) {
                                    item.fillData(idata[item[self.key]()]);
                                    item.loading(false);
                                    if (dataset.dependencyLoader !== undefined) {
                                        dataset.dependencyLoader(item);
                                    }
                                }
                            });
                            if (dataset.overviewLoader !== undefined) {
                                dataset.overviewLoader(keys);
                            }
                        }
                        self.pageLoading(false);
                    });
                if (preload) {
                    self.preloadHandle = promise;
                } else {
                    self.loadHandle = promise;
                }
            }
        };

        // Durandal
        self.activate = function(settings) {
            if (!settings.hasOwnProperty('headers')) {
                throw 'Headers should be specified';
            }

            if (settings.hasOwnProperty('items')) {
                self.external(true);
            }
            if (settings.hasOwnProperty('container')) {
                self.container(settings.container);
            } else {
                self.container(ko.observableArray([]));
            }
            self.loadData = generic.tryGet(settings, 'loadData');
            self.refresh = parseInt(generic.tryGet(settings, 'refreshInterval', '5000'), 10);
            self.key = generic.tryGet(settings, 'key', 'guid');
            self.settings(settings);
            self.headers(settings.headers);
            self.controls(generic.tryGet(settings, 'controls', true));

            if (settings.hasOwnProperty('trigger')) {
                settings.trigger.subscribe(function() { self.load(self.current(), false); });
            }

            self.refresher.init(function() {
                self.load(self.current(), false);
                if (self.hasLoad(true, false)) {
                    return;
                }
                self.preloadPage += 1;
                if (self.preloadPage === self.current()) {
                    self.preloadPage += 1;
                }
                if (self.preloadPage > self.lastPage()) {
                    self.preloadPage = 1;
                }
                if (self.preloadPage !== self.current()) {
                    self.load(self.preloadPage, true);
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
