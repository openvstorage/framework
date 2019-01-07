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
        self.canChangePreset = true;

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
            if (!vpool || (vpool.backendPolicies().length === 0 && !vpool.backendPreset)) {
               return undefined
            }
            return backendService.enhancePreset(vpool.backendPreset(), vpool.backendPolicies());
        });
        self.enhancedPresets = ko.pureComputed(function() {
            var presets = !self.backend() ? [] : self.backend().presets;
            return backendService.parsePresets(presets)
        });
        self.preset = ko.computed({
            deferEvaluation: true,  // Wait with computing for an actual subscription
            read: function() {
                var backendInfo = self.getBackendInfo();
                /**
                 * Race condition hotfix for VPool extend, gather_vpool:
                 * Its possible that the backends haven't loaded when this model is instantiated
                 * This computed would return 'undefined' and the dropdown widget would detect that the current value
                 * isn't in the possible item set, setting the first value back
                 * Which may result in the wrong preset being displayed
                 * We could either wait for the backends to be completely loaded before showing the user this information
                 * or cheat a little bit as the preset information is present in the passed vpool object
                 * I chose the cheating option - using canChangePreset
                 */
                if (!self.canChangePreset) {
                    // Mimick the parsedPreset item. Colours are not displayed.
                    return {
                        'name': backendInfo.preset(),
                        'policies': backendInfo.policies().map(function(policy){
                            return backendService.parsePolicy(policy)
                        })
                    }
                }
                var preset = self.data.getPreset(backendInfo.alba_backend_guid(), backendInfo.preset());
                if (!preset || !preset.name) {
                    return undefined
                }
                return backendService.parsePreset(preset);
            },
            write: function(preset) {
                var backendInfo = self.getBackendInfo();
                // Might be undefined if the available presets change and the dropdown clears the input
                preset = preset || {'name': undefined};
                backendInfo.preset(preset.name);
            }
        });

        // Functions
        /**
         * Load all available backends given the current connection info
         * Used by the UI
         * @return {Promise}
         */
        self.loadBackends = function() {
            return self.data.loadBackends(self.getConnectionInfo())
        };
        self.getDisplayAblePreset = function(item) {
            var policies = [];
            if (item) {
                policies = item.policies.map(function(policy) {
                    return policy.text
                });
                return item.name + ': ' + policies.join(', ');
            }
        };
        /**
         * Get the connection info
         * @return {object}
         */
        self.getConnectionInfo = function() {
            return self.getBackendInfo().connection_info
        };
        /**
         * Get the backend info
         * @return {object}
         */
        // Abstract. Requires implementations
        self.getBackendInfo = function() {
            throw new Error("Method must be implemented.");
        };

    }
    return BaseStep;
});
