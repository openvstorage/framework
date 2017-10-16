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
    'ovs/generic',
    'viewmodels/containers/shared/base_container'
], function($, ko, generic, BaseModel) {
    "use strict";
    // Caching data viewModel which is parsed from JS
    // Return a constructor for a nested viewModel
    var backendInfoMapping = {
        'connection_info': {
            create: function (options) {
                if (options.data !== null) return new connectionInfoViewModel(options.data);
            }
        }
    };
    var connectionInfoMapping = {};
    var backendInfoViewModel = function(data) {
        var self = this;
        // Observables (This will ensure that these observables are present even if the data is missing them)
        self.name                = ko.observable();
        self.backend_guid        = ko.observable();
        self.alba_backend_guid   = ko.observable();
        self.policies            = ko.observableArray([]);
        self.preset              = ko.observable();

        // Default data
        var vmData = $.extend({
            // Placing the observables declared above to automatically include them into our mapping object (else include must be used in the mapping)
            name: undefined,
            backend_guid: undefined,
            alba_backend_guid: undefined,
            policies: [],
            preset: undefined,
            connection_info: {}
        }, data);

        ko.mapping.fromJS(vmData, backendInfoMapping, self);

        // Computed
        self.enhancedPreset = ko.pureComputed(function() {
            /**
             * Compute a preset to look like presetName: (1,1,1,1),(2,1,2,1)
             */
            if (self.policies() === undefined || self.preset() === undefined) {
                return null;
            }
            var policies = [];
            ko.utils.arrayForEach(self.policies(), function (policy) {
                policies.push('(' + policy.join(', ') + ')')
            });
            return ko.utils.unwrapObservable(self.preset) + ': ' + policies.join(', ');
        });
        self.isLocalBackend = ko.pureComputed(function() {
            return self.connection_info.isLocalBackend()
        });

        // Inherit from base
        BaseModel.call(self);

    };
    var connectionInfoViewModel = function(data) {
        var self = this;
        // Observables
        self.client_id      = ko.observable().extend({removeWhiteSpaces: null});
        self.client_secret  = ko.observable().extend({removeWhiteSpaces: null});
        self.host           = ko.observable().extend({regex: generic.hostRegex});
        self.port           = ko.observable().extend({ numeric: {min: 1, max: 65535}});
        self.local          = ko.observable();

        // Default data
        var vmData = $.extend({
            // Placing the observables declared above to automatically include them into our mapping object (else include must be used in the mapping)
            client_id: undefined,
            client_secret: undefined,
            host: undefined,
            port: undefined,
            local: undefined
        }, data);

        ko.mapping.fromJS(vmData, connectionInfoMapping, self);

        // Computed
        self.isLocalBackend = ko.computed({
           deferEvaluation: true,  // Wait with computing for an actual subscription
           read: function() {
               if (self.local() === undefined) {
                   // Default to True for reading purposes
                   return true;
            }
            return self.local();
           },
           write: function(value) {
               self.local(value)
           }
        });
        self.hasRemoteInfo = ko.pureComputed(function (){
            var requiredProps = [self.client_id, self.client_secret, self.host, self.port];
            var hasRemoteInfo = true;
            $.each(requiredProps, function(index, prop) {
                if (ko.utils.unwrapObservable(prop) === undefined){
                    hasRemoteInfo = false;
                    return false  // Break
                }
            });
            return hasRemoteInfo;
        });

        // Inherit from base
        BaseModel.call(self);
    };
    return backendInfoViewModel;
});
