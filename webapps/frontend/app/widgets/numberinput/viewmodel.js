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
    'knockout', 'jquery'
], function(ko, $) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.boundItem = null;

        // Functions
        self.increment = function() {
            self.boundItem(self.boundItem() + 1);  // Relying on the extender for min and max
        };
        self.decrement = function() {
          self.boundItem(self.boundItem() - 1);  // Relying on the extender for min and max
        };

        // Durandal
        self.activate = function(settings) {
            if (!settings.hasOwnProperty('item')) {
                throw 'An item has to be specified';
            }
            if (!ko.isObservable(settings.item)) {
                throw 'The item should be an observable'
            }
            self.boundItem = settings.item;
        };
    };
});
