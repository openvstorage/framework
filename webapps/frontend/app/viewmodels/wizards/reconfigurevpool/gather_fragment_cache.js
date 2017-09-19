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
    'ovs/api', 'ovs/generic', 'ovs/shared',
    './data'
], function($, ko, api, generic, shared, data) {
    "use strict";
    return function(options) {
        var self = this;

        // Variables
        self.actived = false;
        self.data = options !== undefined && options.data !== undefined ? options.data : data;
        self.shared = shared;
        self.options = options;

        // Subscriptions
        self.subscriptions = [];
        // Observables
        self.fragmentCacheSettings      = ko.observableArray(['write', 'read', 'rw', 'none']);
        self.preset                     = ko.observable();

        // Computed
        self.canContinue = ko.computed(function() {
            var reasons = [], fields = [];
            return { value: reasons.length === 0, reasons: reasons, fields: fields };
        });
        self.fragmentCacheBackend = ko.computed({
            read: function() {
                var backendInfo = self.data.cachingData.fragmentCache.backendInfo;
                return self.data.getBackend(backendInfo.backend_guid());
            },
            write: function(backend) {
                // Mutate the backend info
                var backendInfo = self.data.cachingData.fragmentCache.backendInfo;
                backendInfo.name(backend.name);
                backendInfo.backend_guid(backend.backend_guid);
                backendInfo.alba_backend_guid(backend.guid);
            }
        });
        self.fragmentCacheBackends = ko.computed(function() {
            var backendInfo = self.data.cachingData.fragmentCache.backendInfo;
            var connectionInfo = backendInfo.connectionInfo;
            var locationKey = self.data.buildLocationKey(connectionInfo);
            self.data.backendsFilter(locationKey);
            return self.data.filteredBackends();
        });
        self.localBackendsAvailable = ko.computed(function() {
            var connectionInfo = self.data.cachingData.fragmentCache.backendInfo.connectionInfo;
            var useLocalBackend = !!ko.utils.unwrapObservable(connectionInfo.local);
            var localBackendsRequiredAmount = useLocalBackend === true ? 2 : 1;
            return Object.keys(self.data.filterBackendsByLocationKey('local')).length >= localBackendsRequiredAmount;
        });
        self.enhancedPresets = ko.computed(function() {
            return self.data.parsePresets(self.fragmentCacheBackend())
        });
        self.preset = ko.computed({
            read: function() {
                var parsedPreset = undefined;
                if (self.fragmentCacheBackend() === undefined) {
                    return parsedPreset
                }
                var backendInfo = self.data.cachingData.fragmentCache.backendInfo;
                if (ko.utils.unwrapObservable(backendInfo.preset) === undefined) {
                    return parsedPreset;
                }
                var preset = self.data.getPreset(backendInfo.alba_backend_guid(), backendInfo.preset());
                if (preset === undefined) {
                    // No preset could be found for our current setting. Attempt to reconfigure it
                    var enhancedPresets = self.enhancedPresets();
                    if (enhancedPresets.length > 0) {
                        parsedPreset = enhancedPresets[0];
                        backendInfo.preset(enhancedPresets[0].name);  // This will trigger this compute to trigger again but also correct the mistake
                    }
                    return parsedPreset
                }
                return self.data.parsePreset(preset);
            },
            write: function(preset) {
                console.log(preset);
                var backendInfo = self.data.cachingData.fragmentCache.backendInfo;
                backendInfo.preset(preset.name);
            }
        });

        // Functions
        self.loadBackends = function() {
            var connectionInfo = self.data.cachingData.fragmentCache.backendInfo.connectionInfo;
            return self.data.loadBackends(connectionInfo)
        };
        self.resetBackend = function() {
            // Will force to recompute everything
            var backendInfo = self.data.cachingData.fragmentCache.backendInfo;
            backendInfo.name(undefined);
            backendInfo.backend_guid(undefined);
            backendInfo.alba_backend_guid(undefined);
            self.resetPreset()
        };
        self.resetPreset = function() {
            var backendInfo = self.data.cachingData.fragmentCache.backendInfo;
            backendInfo.preset(undefined);
            backendInfo.policies([]);
        };

        // Durandal
        self.activate = function() {
            if (self.actived === false) {
                self.data.loadBackends();
                var connectionInfo = self.data.cachingData.fragmentCache.backendInfo.connectionInfo;
                if (!!ko.utils.unwrapObservable(connectionInfo.local) === false && !['', undefined, null].contains(connectionInfo().host())) {
                    self.data.loadBackends(connectionInfo);
                }
                self.subscriptions.push(ko.watch(connectionInfo, {depth: 1}, function(parents, child, item) {
                    // Reset our information when any change is detected in theconnection information
                    self.resetBackend()
                }));
                self.actived = true;
            }
        };
        self.deactivate = function() {
            $.each(self.subscriptions, function(index, subscription) {
               subscription.dispose();
            });
        }
    }
});
