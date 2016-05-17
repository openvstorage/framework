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
    'knockout',
    'ovs/generic'
], function(ko, generic) {
    "use strict";
    return function(guid) {
        var self = this;

        // Observables
        self.guid       = ko.observable(guid);
        self.name       = ko.observable();
        self.component  = ko.observable();
        self.validUntil = ko.observable();
        self.data       = ko.observable();
        self.signature  = ko.observable();
        self.canRemove  = ko.observable();
        self.token      = ko.observable();
        self.loading    = ko.observable(false);

        // Functions
        self.fillData = function(data) {
            self.name(data.name);
            self.component(data.component);
            self.data(data.data);
            self.token(data.token);
            self.validUntil(data.valid_until);
            self.signature(data.signature);
            generic.trySet(self.canRemove, data, 'can_remove');
            self.loading(false);
        };
    };
});
