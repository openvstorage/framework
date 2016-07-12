// Copyright (C) 2016 iNuron NV
//
// This file is part of Open vStorage Open Source Edition (OSE),
// as available from
//
//      http://www.openvstorage.org and
//      http://www.openvstorage.com.
//
// This file is free software; you can redistribute it and/or modify it
// under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
// as published by the Free Software Foundation, in version 3 as it comes
// in the LICENSE.txt file of the Open vStorage OSE distribution.
//
// Open vStorage is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY of any kind.
/*global define */
define([
    'jquery', 'knockout',
    'ovs/generic'
], function($, ko, generic) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.itemsLoaded = '';
        self.loadedObservable = '';

        // Observables
        self.colspan     = ko.observable(0);
        self.displaymode = ko.observable('span');
        self.items       = ko.observableArray([]);

        // Functions
        self.isLoaded = function(observable) {
            return observable[self.loadedObservable]();
        };
        self.itemsHaveBeenLoaded = function(observable) {
            if (observable.hasOwnProperty(self.itemsLoaded)) {
                return observable[self.itemsLoaded]();
            }
            return true;
        };

        // Durandal
        self.activate = function(settings) {
            if (!settings.hasOwnProperty('items')) {
                throw 'Items should be specified';
            }
            self.displaymode(generic.tryGet(settings, 'displaymode', 'span'));
            self.colspan(generic.tryGet(settings, 'colspan', 0));
            self.loadedObservable = generic.tryGet(settings, 'loadedObservable', 'initialized');
            self.itemsLoaded = generic.tryGet(settings, 'itemsLoaded', undefined);
            self.items = settings.items;
        };
    };
});
