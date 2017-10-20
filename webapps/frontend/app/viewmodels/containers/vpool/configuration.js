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
    'jquery', 'knockout', 'durandal/app', 'ovs/generic',
    'viewmodels/containers/shared/base_container', 'viewmodels/services/storagedriver'
], function($, ko, app, generic, BaseModel, storageDriverService) {
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
        self.write_buffer       = ko.observable()   // Volume Write buffer
            .extend({numeric: {min: 128, max: 10240, allowUndefined: false, validate: true},
                     rateLimit: { method: "notifyWhenChangesStop", timeout: 400}});
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

        // Subscriptions
        self.sco_size.subscribe(function(newValue) {
            app.trigger('vpool_configuration:update', $.extend(self.advancedSettings(), {sco_size: newValue}));
        });
        self.write_buffer.subscribe(function(newValue) {
            // Notify other models when write buffer changes
             app.trigger('vpool_configuration:update', $.extend(self.advancedSettings(), {write_buffer: newValue}));
        });

        // Computed
        self.advancedSettings = ko.computed({
            // Compute propagate changes to sco size and write buffer
            read: function() {
                return {
                    'sco_size': self.sco_size(),
                    'write_buffer': self.write_buffer()
                };
            },
            write: function(newSettings) {
                // Determine if the values changed
                if (generic.objectEquals(self.advancedSettings(), newSettings)) {
                    return
                }
                self.sco_size(newSettings.sco_size);
                self.write_buffer(newSettings.write_buffer);
           }
        });

        // Functions
        self.subscribeConfigurations = function() {
            var advancedUpdateSub = app.on('vpool_configuration_advanced:update').then(function(data) {
                // Update the write buffer when the advanced config changes
                var advancedSettings = {
                    sco_size: self.sco_size(),
                    write_buffer: storageDriverService.calculateVolumeWriteBuffer(data.number_of_scos_in_tlog, data.non_disposable_scos_factor, self.sco_size())
                };
                self.advancedSettings(advancedSettings);
            });
            self.eventSubscriptions.push(advancedUpdateSub);
        };

        self.unSubscribeConfigurations = function() {
            $.each(self.eventSubscriptions(), function(index, subscription){
                subscription.off();
            });
            self.eventSubscriptions.removeAll()
        };

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

        // Properties
        var eventTriggeredUpdate = false;  // Keep track if the update happens because of direct editing or through the advancedSettings computed
        // Observables
        self.number_of_scos_in_tlog = ko.observable()
            .extend({numeric: {min: 4, max: 10240, allowUndefined: false, validate: true},
                     rateLimit: { method: "notifyWhenChangesStop", timeout: 400}});
        self.non_disposable_scos_factor = ko.observable()
            .extend({numeric: {min: 128, max: 10240, allowUndefined: false, validate: true},
                     rateLimit: { method: "notifyWhenChangesStop", timeout: 400}});
        // Event subscriptions
        self.eventSubscriptions = ko.observableArray();

        // Default data
        var vmData = $.extend({
            number_of_scos_in_tlog: undefined,
            non_disposable_scos_factor: undefined
        }, data);

        // Bind the data into this
        ko.mapping.fromJS(vmData, {}, self);

        // Subscription
        self.advancedSettings = ko.computed(function() {
            // This computed is used to subscribe on multiple observables and not used to return a value
            var advancedSettings = {
                'number_of_scos_in_tlog': self.number_of_scos_in_tlog(),
                'non_disposable_scos_factor': self.non_disposable_scos_factor()
            };
            if (eventTriggeredUpdate === false) {
                app.trigger('vpool_configuration_advanced:update', advancedSettings);
            }
            // Reset
            eventTriggeredUpdate = false;
        });

        // Functions
        self.subscribeConfigurations = function() {
            var advancedUpdateSub = app.on('vpool_configuration:update').then(function(settings) {
                eventTriggeredUpdate = true;
                // Update the configuration
                self.updateSettings(storageDriverService.calculateAdvancedFactors(settings.sco_size, settings.write_buffer));
            });
            self.eventSubscriptions.push(advancedUpdateSub);
        };
        self.unSubscribeConfigurations = function() {
            $.each(self.eventSubscriptions(), function(index, subscription){
                subscription.off();
            });
            self.eventSubscriptions.removeAll()
        };
        self.updateSettings = function(newSettings) {
            // Used to update the view models settings when the event emitted
            if (generic.objectEquals(self.advancedSettings(), newSettings)) {
                return
            }
            self.update(newSettings);
            // self.number_of_scos_in_tlog(newSettings.number_of_scos_in_tlog);
            // self.non_disposable_scos_factor(newSettings.non_disposable_scos_factor);
       }
    };
    return ConfigurationViewModel;
});
