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
    'knockout', 'ovs/generic', 'ovs/shared'
], function(ko, generic, shared) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared = shared;

        // Observables
        self.dataLoading     = ko.observable(false);
        self.widgetActivated = ko.observable(false);
        self.data            = ko.observable();

        // Computed
        self.hasData         = ko.computed(function() {
            return !(
                (!self.widgetActivated()) ||  // The widget is not loaded yet
                (!self.data()) ||             // The current observable is not set
                // The observed data is not set, or an empyt list
                (!self.data()() || (self.data()().hasOwnProperty('length') && self.data()().length === 0))
            );
        }).extend({ rateLimit: 50 });

        // Durandal
        self.activate = function(settings) {
            if (!settings.hasOwnProperty('data')) {
                throw 'Data should be specified';
            }
            self.data = settings.data;
            self.widgetActivated(true);
        };
    };
});
