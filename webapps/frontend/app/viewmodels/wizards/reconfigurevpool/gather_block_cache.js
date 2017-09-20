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

        // Observables
        self.blockCacheSettings      = ko.observableArray(['write', 'read', 'rw', 'none']);
        self.preset                     = ko.observable();

        // Computed
        self.blockCacheBackend = ko.computed({
            deferEvaluation: true,  // Wait with computing for an actual subscription
            read: function() {
                var backendInfo = self.data.cachingData.blockCache.backendInfo;
                var backend = self.data.getBackend(backendInfo.backend_guid());
                if (backend === undefined) {
                    // Return the first of the list
                    var backends = self.getblockCacheBackends();
                    if (backends !== undefined && backends.length > 0) {
                        backend = backends[0];
                        self.blockCacheBackend(backend)
                    }
                }
                return backend;
            },
            write: function(backend) {
                // Mutate the backend info
                var backendInfo = self.data.cachingData.blockCache.backendInfo;
                backendInfo.name(backend.name);
                backendInfo.backend_guid(backend.backend_guid);
                backendInfo.alba_backend_guid(backend.guid);
            }
        });
        self.blockCacheBackends = ko.computed({
            deferEvaluation: true,  // Wait with computing for an actual subscription
            read: function () {
                var backends = self.getblockCacheBackends();
                if (backends.length === 0) {
                    // Update our Model
                    self.resetBackend();
                }
                return backends;
            }
        });
        self.localBackendsAvailable = ko.pureComputed(function() {
            var connectionInfo = self.data.cachingData.blockCache.backendInfo.connectionInfo;
            var useLocalBackend = !!ko.utils.unwrapObservable(connectionInfo.isLocalBackend);
            var localBackendsRequiredAmount = useLocalBackend === true ? 2 : 1;
            return self.data.filterBackendsByLocationKey('local').length >= localBackendsRequiredAmount;
        });
        self.enhancedPresets = ko.pureComputed(function() {
            return self.data.parsePresets(self.blockCacheBackend())
        });
        self.preset = ko.computed({
            deferEvaluation: true,  // Wait with computing for an actual subscription
            read: function() {
                var parsedPreset = undefined;
                if (self.blockCacheBackend() === undefined) {
                    return parsedPreset
                }
                var backendInfo = self.data.cachingData.blockCache.backendInfo;
                var preset = self.data.getPreset(backendInfo.alba_backend_guid(), backendInfo.preset());
                if (preset === undefined) {
                    // No preset could be found for our current setting. Attempt to reconfigure it
                    var enhancedPresets = self.enhancedPresets();
                    if (enhancedPresets.length > 0) {
                        parsedPreset = enhancedPresets[0];
                        self.preset(parsedPreset);  // This will trigger this compute to trigger again but also correct the mistake
                    }
                    return parsedPreset
                }
                return self.data.parsePreset(preset);
            },
            write: function(preset) {
                var backendInfo = self.data.cachingData.blockCache.backendInfo;
                backendInfo.preset(preset.name);
            }
        });
        self.canContinue = ko.pureComputed(function() {
            var reasons = [], fields = [];
            var blockCache = self.data.cachingData.blockCache;
            if (blockCache.isUsed() === true){
                if (self.data.loadingBackends() === true) {
                    reasons.push($.t('ovs:wizards.reconfigure_vpool.gather_fragment_cache.backends_loading'));
                } else {
                    if (blockCache.isBackend() === true ){
                        if (self.blockCacheBackend() === undefined) {
                            reasons.push($.t('ovs:wizards.reconfigure_vpool.gather_fragment_cache.choose_backend'));
                            fields.push('backend');
                        }
                        else if (self.preset() === undefined) {
                            reasons.push($.t('ovs:wizards.reconfigure_vpool.gather_fragment_cache.choose_preset'));
                            fields.push('preset');
                        }
                    }
                }
            }
            return { value: reasons.length === 0, reasons: reasons, fields: fields };
        });

        // Functions
        self.loadBackends = function() {
            var connectionInfo = self.data.cachingData.blockCache.backendInfo.connectionInfo;
            return self.data.loadBackends(connectionInfo)
        };
        self.resetBackend = function() {
            // Will force to recompute everything
            self.blockCacheBackend({'name': undefined, 'backend_guid':undefined, 'alba_backend_guid': undefined});
            self.resetPreset();
        };
        self.resetPreset = function() {
            self.preset({'name': undefined});
        };
        self.getblockCacheBackends = function() {
            // Wrapped function for the computable
            // Issue was when the computed would update the Model when no backends were found, the computed would not
            // return its value and the backend computed would fetch the old values, causing a mismatch
            var backendInfo = self.data.cachingData.blockCache.backendInfo;
            var connectionInfo = backendInfo.connectionInfo;
            return self.data.filterBackendsByLocationKey(self.data.buildLocationKey(connectionInfo));
        };

        // Durandal
        self.activate = function() {
            if (self.actived === false) {
                self.data.loadBackends();
                var connectionInfo = self.data.cachingData.blockCache.backendInfo.connectionInfo;
                if (!!ko.utils.unwrapObservable(connectionInfo.isLocalBackend) === false && !['', undefined, null].contains(connectionInfo.host())) {
                    self.data.loadBackends(connectionInfo);
                }
                self.actived = true;
            }
        };
    }
});
