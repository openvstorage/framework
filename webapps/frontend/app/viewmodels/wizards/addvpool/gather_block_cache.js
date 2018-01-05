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
    'viewmodels/services/backend'
], function($, ko, api, generic, shared, backendService) {
    "use strict";
    return function(options) {
        var self = this;

        // Variables
        self.actived = false;
        self.data = options.data;
        self.shared = shared;
        self.options = options;

        // Observables
        self.blockCacheSettings      = ko.observableArray(['write', 'read', 'rw', 'none']);
        self.preset                  = ko.observable();
        self._reUsedStorageRouter    = ko.observable();

        // Computed
        self.blockCacheBackend = ko.computed({
            deferEvaluation: true,  // Wait with computing for an actual subscription
            read: function() {
                var backendInfo = self.data.cachingData.block_cache.backend_info;
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
                var backendInfo = self.data.cachingData.block_cache.backend_info;
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
        self.enhancedPresets = ko.pureComputed(function() {
            var presets = self.blockCacheBackend() === undefined ? [] : self.blockCacheBackend().presets;
            return backendService.parsePresets(presets)
        });
        self.preset = ko.computed({
            deferEvaluation: true,  // Wait with computing for an actual subscription
            read: function() {
                var parsedPreset = undefined;
                if (self.blockCacheBackend() === undefined) {
                    return parsedPreset
                }
                var backendInfo = self.data.cachingData.block_cache.backend_info;
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
                return backendService.parsePreset(preset);
            },
            write: function(preset) {
                var backendInfo = self.data.cachingData.block_cache.backend_info;
                backendInfo.preset(preset.name);
            }
        });
        self.canContinue = ko.pureComputed(function() {
            var reasons = [], fields = [];
            var blockCache = self.data.cachingData.block_cache;
            if (blockCache.isUsed() === true){
                if (self.data.loadingBackends() === true) {
                    reasons.push($.t('ovs:wizards.reconfigure_vpool.gather_block_cache.backends_loading'));
                } else {
                    var connectionInfo = blockCache.backend_info.connection_info;
                    if (blockCache.is_backend() === true ){
                        if (self.blockCacheBackend() === undefined) {
                            reasons.push($.t('ovs:wizards.reconfigure_vpool.gather_block_cache.choose_backend'));
                            fields.push('backend');
                        }
                        else if (self.preset() === undefined) {
                            reasons.push($.t('ovs:wizards.reconfigure_vpool.gather_block_cache.choose_preset'));
                            fields.push('preset');
                        }
                        if (connectionInfo.isLocalBackend() === false && connectionInfo.hasRemoteInfo() === false || self.data.invalidBackendInfo() === true) {
                            reasons.push($.t('ovs:wizards.reconfigure_vpool.gather_block_cache.invalid_alba_info'));
                            fields.push('invalid_alba_info');
                        }
                    }
                }
            }
            return { value: reasons.length === 0, reasons: reasons, fields: fields };
        });
        self.reUsedStorageRouter = ko.computed({
            deferEvaluation: true,  // Wait with computing for an actual subscription
            read: function() {
                return self._reUsedStorageRouter()
            },
            write: function(data) {
                self._reUsedStorageRouter(data);
                // Set connection info
                if (data !== undefined) {
                    var connectionInfo = self.data.vPool().getCacheConnectionInfoMapping().block_cache[data.guid()];
                    self.data.cachingData.block_cache.backend_info.connection_info.update(connectionInfo)
                }
            }
        });

        // Functions
        self.loadBackends = function() {
            var connectionInfo = self.data.cachingData.block_cache.backend_info.connection_info;
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
            var backendInfo = self.data.cachingData.block_cache.backend_info;
            var connectionInfo = backendInfo.connection_info;
            // Filter out the chosen backend for the vpool
            var backends = self.data.filterBackendsByLocationKey(self.data.buildLocationKey(connectionInfo));
            backends = backends.filter(function(backend) {
                // Working with alba backend objects, so guid == alba_backend_guid
                if (backend.guid !== self.data.backendData.backend_info.alba_backend_guid()) { return backend }
            });
            return backends;
        };

        // Durandal
        self.activate = function() {
            if (self.actived === false) {
                var connectionInfo = self.data.cachingData.block_cache.backend_info.connection_info;
                if (!!ko.utils.unwrapObservable(connectionInfo.isLocalBackend) === false && !['', undefined, null].contains(connectionInfo.host())) {
                    self.data.loadBackends(connectionInfo);
                }
                self.actived = true;
            }
        };
    }
});
