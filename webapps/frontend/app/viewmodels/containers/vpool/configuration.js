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
    'jquery', 'knockout', 'durandal/app',
    'viewmodels/containers/shared/base_container', 'viewmodels/services/storagedriver'
], function($, ko, app, BaseModel, storageDriverService) {
    "use strict";
    // Configuration viewModel which is parsed from JS
    // Return a constructor for a nested viewModel
    var configurationMapping = {
       'mds_config': {
            create: function (options) {
                if (options.data !== null) return new MDSConfigModel(options.data);
            }
        },
        'advanced_config': {
          create: function(options) {
              if (options.data !== null) return new AdvancedConfigModel(options.data);
          }
        },
        'ignore': ["clusterSizes", "dtlModes", "dtlTransportModes", "scoSizes"]
    };
    var ConfigurationViewModel = function(data) {
        var self = this;
        // Inherit
        BaseModel.call(self);

        // Properties
        self._advancedSettings  = undefined;  // Used for caching
        // Observables
        // Properties returned by the api:
        self.dtl_config_mode    = ko.observable();
        self.dtl_enabled        = ko.observable();
        self.dtl_mode           = ko.observable();
        self.dtl_transport      = ko.observable();
        self.sco_size           = ko.observable();
        self.tlog_multiplier    = ko.observable();  // Number of sco's in tlog - returned by the vpool metadata
        self.write_buffer       = ko.observable().extend({numeric: {min: 128, max: 10240, allowUndefined: true, validate: true}});  // Volume Write buffer
        // Own properties
        self.advanced           = ko.observable(false);  // Make use of the advanced config
        // Event subscriptions
        self.eventSubscriptions = ko.observableArray();

        // Default data
        var vmData = $.extend({
            mds_config: {},
            advanced_config: {
                number_of_scos_in_tlog: data.tlog_multiplier  // Map to the advanced config view
            }
        }, data);

        // Constants
        self.clusterSizes       = [4, 8, 16, 32, 64];
        self.dtlModes           = ['no_sync', 'a_sync', 'sync'];
        self.dtlTransportModes  = ['tcp', 'rdma'];
        self.scoSizes           = [4, 8, 16, 32, 64, 128];
        self.tlogMultiplierMap  = {  // Maps sco size to a tlog_multiplier
            4: 16,
            8: 8,
            16: 4,
            32: 2,
            64: 1,
            128: 1
        };

        // Computed
        self.advancedSettings = ko.computed(function() {
           // Compute propagate changes to sco size and write buffer
           var scoSize = self.sco_size();
           var writeBuffer = self.write_buffer();
           var advancedSettings = {sco_size: scoSize, write_buffer: writeBuffer};
           app.trigger('vpool_configuration:update', advancedSettings);
           return advancedSettings;
        }).extend({ deferred: true });

        // Functions
        self.subscribeConfigurations = function() {
            var advancedUpdateSub = app.on('vpool_configuration_advanced:update').then(function(data) {
                // Update the write buffer when the advanced config changes
                self.write_buffer(storageDriverService.calculateVolumeWriteBuffer(data.number_of_scos_in_tlog, data.non_disposable_scos_factor).volume_write_buffer)
            });
            self.eventSubscriptions.push(advancedUpdateSub);
        };

        self.unsubscribeConfigurations = function() {
            $.each(self.eventSubscriptions(), function(index, subscription){
                subscription.off();
            });
            self.eventSubscriptions.removeAll()
        };

        // Subscribe by default
        self.subscribeConfigurations();
        // Bind the data into this
        ko.mapping.fromJS(vmData, configurationMapping, self);
    };
    var MDSConfigModel = function(data) {
        var self = this;
        // Observables (This will ensure that these observables are present even if the data is missing them)
        self.mds_maxload        = ko.observable();
        self.mds_tlogs          = ko.observable();
        self.mds_safety         = ko.observable().extend({ numeric: {min: 1, max: 5}});

        ko.mapping.fromJS(data, {}, self);
    };

    var AdvancedConfigModel = function(data) {
        var self = this;

        // Inherit
        BaseModel.call(self);

        // Observables
        self.number_of_scos_in_tlog =       ko.observable();
        self.non_disposable_scos_factor =   ko.observable();
        // Event subscriptions
        self.eventSubscriptions = ko.observableArray();

        // Default data
        var vmData = $.extend({
            number_of_scos_in_tlog: undefined,
            non_disposable_scos_factor: undefined
        }, data);

        // Functions
        self.subscribeConfigurations = function() {
            var advancedUpdateSub = app.on('vpool_configuration:update').then(function(settings) {
                // Update the configuration
                ko.mapping.fromJS(storageDriverService.calculateAdvancedFactors(settings.sco_size, settings.write_buffer), self);
            });
            self.eventSubscriptions.push(advancedUpdateSub);
        };

        self.unsubscribeConfigurations = function() {
            $.each(self.eventSubscriptions(), function(index, subscription){
                subscription.off();
            });
            self.eventSubscriptions.removeAll()
        };

        // Subscribe by default
        self.subscribeConfigurations();
        // Bind the data into this
        ko.mapping.fromJS(data, {}, self);

    };
    return ConfigurationViewModel;
});
