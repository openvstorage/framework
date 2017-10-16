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
    'jquery', 'knockout'
], function($, ko) {
    "use strict";
    // Configuration viewModel which is parsed from JS
    // Return a constructor for a nested viewModel
    var configurationMapping = {
       'mds_config': {
            create: function (options) {
                if (options.data !== null) return new MDSConfigModel(options.data);
            }
        },
        'ignore': ["clusterSizes", "dtlModes", "dtlTransportModes", "scoSizes"]
    };
    var ConfigurationViewModel = function(data) {
        var self = this;
        // Observables (This will ensure that these observables are present even if the data is missing them)
        // Properties returned by the api:
        self.dtl_config_mode    = ko.observable();
        self.dtl_enabled        = ko.observable();
        self.dtl_mode           = ko.observable();
        self.dtl_transport      = ko.observable();
        self.sco_size           = ko.observable();
        self.tlog_multiplier    = ko.observable();
        self.write_buffer       = ko.observable().extend({numeric: {min: 1, max: 10240, allowUndefined: true}});  // Volume Write buffer

        var vmData = $.extend({
            mds_config: {}
        }, data);

        // Constants
        self.clusterSizes      = [4, 8, 16, 32, 64];
        self.dtlModes          = ['no_sync', 'a_sync', 'sync'];
        self.dtlTransportModes = ['tcp', 'rdma'];
        self.scoSizes          = [4, 8, 16, 32, 64, 128];

        ko.mapping.fromJS(vmData, configurationMapping, self)  // Bind the data into this
    };
    var MDSConfigModel = function(data) {
        var self = this;
        // Observables (This will ensure that these observables are present even if the data is missing them)
        self.mds_maxload        = ko.observable();
        self.mds_tlogs          = ko.observable();
        self.mds_safety         = ko.observable().extend({ numeric: {min: 1, max: 5}});

        ko.mapping.fromJS(data, {}, self);
    };
    return ConfigurationViewModel;
});
