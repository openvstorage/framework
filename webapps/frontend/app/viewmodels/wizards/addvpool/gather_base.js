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
    'viewmodels/containers/storagerouter/storagerouter', 'viewmodels/containers/storagedriver/storagedriver',
    'viewmodels/services/backend'
], function ($, ko,
             StorageRouter, StorageDriver, backendService) {
    "use strict";

    /**
     * Base container for the vpool steps
     * All backend model options are offloaded to this container to avoid code duplication
     * @param options: Step options given
     * @constructor
     */
    function BaseStep(options) {
        var self = this;

        // Variables
        self.data   = options.data;


        self.backend = ko.computed({
            deferEvaluation: true,  // Wait with computing for an actual subscription
            read: function() {
                return self.data.getBackend(self.getBackendInfo().backend_guid());
            },
            write: function(backend) {
                // Mutate the backend info
                var backendInfo = self.getBackendInfo();
                // Backend might be undefined if the dropdown contents change
                backend = backend || {
                    'name': undefined,
                    'backend_guid': undefined,
                    'alba_backend_guid': undefined
                };
                backendInfo.name(backend.name);
                backendInfo.backend_guid(backend.backend_guid);
                backendInfo.alba_backend_guid(backend.guid);
            }
        });
        self.backends = ko.pureComputed(function() {
            return self.data.filterBackendsByLocationKey(self.data.buildLocationKey(self.getConnectionInfo()));
        });
        self.enhancedPreset = ko.pureComputed(function() {
            /**
             * Compute a preset to look like presetName: (1,1,1,1),(2,1,2,1)
             */
            var vpool = self.data.vPool();
            if (vpool === undefined || (vpool.backendPolicies().length === 0 && vpool.backendPreset === undefined)) {
               return undefined
            }
            return backendService.enhancePreset(vpool.backendPreset(), vpool.backendPolicies());
        });
        self.enhancedPresets = ko.pureComputed(function() {
            var presets = self.backend() === undefined ? [] : self.backend().presets;
            return backendService.parsePresets(presets)
        });
        self.preset = ko.computed({
            deferEvaluation: true,  // Wait with computing for an actual subscription
            read: function() {
                var backendInfo = self.data.backendData.backend_info;
                var preset = self.data.getPreset(backendInfo.alba_backend_guid(), backendInfo.preset());
                if (!preset || preset.name === undefined) {
                    return undefined
                }
                return backendService.parsePreset(preset);
            },
            write: function(preset) {
                var backendInfo = self.data.backendData.backend_info;
                // Might be undefined if the available presets change and the dropdown clears the input
                preset = preset || {'name': undefined};
                backendInfo.preset(preset.name);
            }
        });

        // Functions
        /**
         * Load all available backends given the current connection info
         * Used by the UI
         * @return {*|Promise|Deferred}
         */
        self.loadBackends = function() {
            return self.data.loadBackends(self.getConnectionInfo())
        };
        self.getConnectionInfo = function() {
            return self.getBackendInfo().connection_info
        };
        // Abstract. Requires implementations
        self.getBackendInfo = function() {
            throw new Error("Method must be implemented.");
        };

    }
    return BaseStep;
});
