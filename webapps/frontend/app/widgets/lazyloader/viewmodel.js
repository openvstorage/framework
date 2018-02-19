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
        self.isLoaded = ko.pureComputed(function() {
            // Item can either by a native object, observable or a viewModel with observable properties
            var item = ko.utils.unwrapObservable(self._item());
            if ([undefined, null].contains(item)){
                return false;
            }
            if (item.hasOwnProperty(self.loadedObservable)) {
                return item[self.loadedObservable]();
            }
            return true;
        });
        self.item = ko.pureComputed(function() {
            return ko.utils.unwrapObservable(self._item());
        });
        self.itemHasValue = ko.pureComputed(function() {
            return ![null, undefined].contains(ko.utils.unwrapObservable(self._item())) || self.undefinedLoading()
        });

        self.itemIsUndefined = ko.pureComputed(function() {
           return ko.utils.unwrapObservable(self._item()) === undefined && !self.undefinedLoading()
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
