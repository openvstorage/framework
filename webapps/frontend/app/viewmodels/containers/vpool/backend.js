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
    'ovs/generic', './shared/backend_info'
], function($, ko, generic, backendInfoViewModel) {
    "use strict";
    // Caching data viewModel which is parsed from JS
    // Return a constructor for a nested viewModel
    var backendMapping = {
        'backend_info': {
            create: function (options) {
                if (options.data !== null) return new backendInfoViewModel(options.data);
            }
        }
    };
    var BackendViewModel = function(data) {
        var self = this;

        // Default data
        var vmData = $.extend({
            // Placing the observables declared above to automatically include them into our mapping object (else include must be used in the mapping)
            backend_info: {},
        }, data);

        ko.mapping.fromJS(data, backendMapping, self);

        // Computed
        self.isLocalBackend = ko.pureComputed(function() {
            return self.backend_info.isLocalBackend()
        })

    };
    return BackendViewModel;
});
