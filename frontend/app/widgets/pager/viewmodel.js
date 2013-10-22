define(['knockout', 'ovs/generic'], function(ko, generic) {
    "use strict";
    return function () {
        var self = this;

        self.viewport_indexes = [];
        self.viewport_shower = undefined;
        self.internal_current = ko.observable(1);

        self.headers = ko.observableArray([]);
        self.settings = ko.observable({});

        self.pagesize = ko.observable(0);
        self.padding = ko.observable(2);
        self.controls = ko.observable(true);

        self.items = ko.computed(function () {
            var settings = self.settings();
            if (settings.hasOwnProperty('items')) {
                return self.settings().items();
            }
            return [];
        });
        self.show_controls = ko.computed(function () {
            return self.controls() || (self.total_items() > self.pagesize());
        });
        self.total_items = ko.computed(function () {
            return self.items().length;
        });
        self.last_page = ko.computed(function () {
            return Math.floor((self.total_items() - 1) / self.pagesize()) + 1;
        });
        self.current = ko.computed({
            // One-based
            read: function() {
                return Math.min(self.internal_current(), Math.floor(self.total_items() / Math.max(1, self.pagesize())) + 1);
            },
            write: function(value) {
                self.internal_current(value);
            }
        });
        self.has_next = ko.computed(function () {
            return self.current() < self.last_page();
        });
        self.has_previous = ko.computed(function () {
            return self.current() > 1;
        });
        self.page_first = ko.computed(function () {
            return (self.current() - 1) * self.pagesize() + 1;
        });
        self.page_last = ko.computed(function () {
            return Math.min(self.page_first() + self.pagesize() - 1, self.items().length);
        });
        self.pages = ko.computed(function () {
            var i,
                pages = [],
                from = Math.max(1, self.current() - self.padding()),
                to = Math.min(self.last_page(), self.current() + self.padding());
            from = Math.max(1, Math.min(to - 2 * self.padding(), from));
            to = Math.min(self.last_page(), Math.max(from + 2 * self.padding(), to));
            for (i = from; i <= to; i += 1) {
                pages.push(i);
            }
            return pages;
        });
        self.viewport_items = ko.computed(function () {
            var i,
                items = self.items(),
                v_items = [],
                v_indexes = [],
                start = (self.current() - 1) * self.pagesize(),
                max = Math.min(start + self.pagesize(), items.length);
            for (i = start; i < max; i += 1) {
                if (self.enter_viewport !== undefined && $.inArray(i, self.viewport_indexes) === -1) {
                    items[i][self.enter_viewport]();
                }
                v_indexes.push(i);
                v_items.push(items[i]);
            }
            self.viewport_indexes = v_indexes.slice();
            return v_items;
        });

        self.step = function(next) {
            if (next) {
                if (self.has_next()) {
                    self.current(self.current() + 1);
                }
            } else {
                if (self.has_previous()) {
                    self.current(self.current() - 1);
                }
            }
        };

        self.activate = function (settings) {
            if (!settings.hasOwnProperty('items')) {
                throw 'Items should be specified';
            }
            if (!settings.hasOwnProperty('headers')) {
                throw 'Headers should be specified';
            }

            self.enter_viewport = generic.tryget(settings, 'enter_viewport');
            self.settings(settings);
            self.headers(settings.headers);
            self.pagesize(settings.pagesize);
            self.controls(generic.tryget(settings, 'controls', true));
        };
    };
});