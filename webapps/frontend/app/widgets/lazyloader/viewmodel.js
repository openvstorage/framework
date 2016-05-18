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
