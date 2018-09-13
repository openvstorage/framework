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
    // Return a constructor for a viewModel
    var configurationMapping = {
        // Not a data view model so these properties have to be explicitly included
        'include': ["storageIP", "proxyAmount", "globalWriteBuffer"]
    };
    var ConfigurationViewModel = function(data) {
        var self = this;
        self.storageIP = ko.observable('').extend({regex: generic.ipRegex});
        self.proxyAmount = ko.observable(2).extend({numeric: {min: 1, max: 16}});
        self.globalWriteBuffer = ko.observable().extend({numeric: {min: 1, max: 10240, allowUndefined: true}, rateLimit: { method: "notifyWhenChangesStop", timeout: 800 }});

        ko.mapping.fromJS(data, configurationMapping, self)  // Bind the data into this
    };
    return ConfigurationViewModel;
});
