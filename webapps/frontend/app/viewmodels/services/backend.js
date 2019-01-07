// Copyright (C) 2017 iNuron NV
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
/**
 * Service to help with backend related tasks
 */
define([
    'jquery', 'knockout',
    'ovs/api', 'ovs/generic'
], function ($, ko, api, generic) {

    var policyColours = ['grey', 'black', 'green'];

    function BackendService() {
        var self = this;
        // Properties
        self.nameRegex = /^[0-9a-z][\-a-z0-9]{1,48}[a-z0-9]$/;

        // Functions
        /**
         * Adds a new backend
         * @param data: Data about the data to add
         */
        self.addBackend = function(data) {
            return api.post('backends', {data: data})
        };

        /**
         * Load all possible backend types
         * @param queryParams: Additional query params. Defaults to no params
         * @param relayParams: Relay to use (Optional, defaults to no relay)
         * @returns {Deferred}
         */
        self.loadBackendTypes = function(queryParams, relayParams){
            return api.get('backendtypes', { queryparams: queryParams, relayParams: relayParams })
        };

        /**
         * Load all backends
         * @param queryParams: Additional query params. Defaults to no params
         * @param relayParams: Relay to use (Optional, defaults to no relay)
         * @returns {Deferred}
         */
        self.loadBackends = function(queryParams, relayParams){
            return api.get('backends', { queryparams: queryParams, relayParams: relayParams })
        };
        /**
         * Compute a preset to look like presetName: (1,1,1,1),(2,1,2,1)
         * @param presetName: Name of the preset
         * @param policies: Array of policies (eg.[[5, 4, 8, 3], [2, 2, 3, 4]])
         */
        self.enhancedPreset = function(presetName, policies) {
            var convertedPolicies = [];
            $.each(policies, function(index, policy) {
                convertedPolicies.push('(' + policy.join(', ') + ')')
            });
            return presetName +': ' + convertedPolicies.join(', ')
        };
        /**
         * Loads in all backends for the current supplied data
         * @param queryParams: Additional query params. Defaults to no params
         * @param relayParams: Relay to use (Optional, defaults to no relay)
         * @returns {Deferred}
         */
        self.loadAlbaBackends = function(queryParams, relayParams) {
            return api.get('alba/backends', { queryparams: queryParams, relayParams: relayParams })
        };
        /**
         * Loads in a backend for the current supplied data
         * @param guid: Guid of the Alba Backend
         * @param queryParams: Additional query params. Defaults to no params
         * @param relayParams: Relay to use (Optional, defaults to no relay)
         * @returns {Deferred}
         */
        self.loadAlbaBackend = function(guid, queryParams, relayParams) {
            return api.get('alba/backends/' + guid + '/', { queryparams: queryParams, relayParams: relayParams });
        };
        /**
         * Parse a policy
         * @param policy: Array representing the policy
         * eg: [5, 4, 8, 3]
         * @param policyMetadata: Metadata about the policy (as returned by the API [Optional]
         * eg {'in_use': False,
              'is_active': False,
              'is_available': False}}
         */
        self.parsePolicy = function(policy, policyMetadata){
            var stringified = '(' + policy.join(', ') +')';
            policyMetadata = policyMetadata || {'is_available': false, 'is_active': false, 'in_use': false};
            var newPolicy = {
                text: stringified,
                color: policyColours[0],
                isActive: policyMetadata.is_active,
                k: policy[0],
                m: policy[1],
                c: policy[2],
                x: policy[3]
            };
            if (policyMetadata.is_available) {
                newPolicy.color = policyColours[1];
            }
            if (policyMetadata.in_use) {
                newPolicy.color = policyColours[2];
            }
            return newPolicy
        };
        /**
         * Parse a preset
         * @param preset: Preset as returned by the api
         * (eg. {'compression': 'snappy',
            'fragment_checksum': ['crc-32c'],
            'fragment_encryption': ['none'],
            'fragment_size': 1048576,
            'in_use': False,
            'is_available': True,
            'is_default': True,
            '               name': 'default',
            'object_checksum': {'allowed': [['none'], ['sha-1'], ['crc-32c']],
             'default': ['crc-32c'],
             'verify_upload': True},
            'osds': ['all'],
            'policies': ['(5, 4, 8, 3)', '(2, 2, 3, 4)'],
            'policy_metadata': {'(2, 2, 3, 4)': {'in_use': False,
              'is_active': True,
              'is_available': True},
             '(5, 4, 8, 3)': {'in_use': False,
              'is_active': False,
              'is_available': False}},
            'version': 0}
         * @return {{policies: Array, name, compression: *, fragSize: *, encryption: *, color: string, inUse: *, isDefault: *, replication: undefined}}
         */
        self.parsePreset = function(preset) {
            var worstPolicy = 0;
            var replication = undefined;
            var policyObject = undefined;
            var policies = preset.policies.map(function(policy){
                policyObject = JSON.parse(policy.replace('(', '[').replace(')', ']'));
                var newPolicy = self.parsePolicy(policyObject, preset.policy_metadata[policy]);
                worstPolicy = Math.max(policyColours.indexOf(newPolicy.color), worstPolicy);
                return newPolicy
            });
            if (preset.policies.length === 1) {
                policyObject = JSON.parse(preset.policies[0].replace('(', '[').replace(')', ']'));
                if (policyObject[0] === 1 && policyObject[0] + policyObject[1] === policyObject[3] && policyObject[2] === 1) {
                    replication = policyObject[0] + policyObject[1];
                }
            }
            return {
                policies: policies,
                name: preset.name,
                compression: preset.compression,
                fragSize: preset.fragment_size,
                encryption: preset.fragment_encryption,
                color: policyColours[worstPolicy],
                inUse: preset.in_use,
                isDefault: preset.is_default,
                replication: replication
            }
        };
        /**
         * Parse multiple presents and sorts
         * @param presets: Array of presets as returned by the api
         * @param sortFunction: Custom sort function to sort the array with (Optional, sorts by name by default, provide null to stop sorting)
         * @return {Array}
         */
        self.parsePresets = function(presets, sortFunction) {
            if (typeof sortFunction === 'undefined') {
                sortFunction = function (preset1, preset2) {
                    return preset1.name.toLowerCase() < preset2.name.toLowerCase() ? -1 : 1;
                }
            } else if (!generic.isFunction(sortFunction) && sortFunction !== null) {
                throw new Error('Provided sort function is not a function')
            }
            var parsedPresets = [];
            $.each(presets, function (index, preset) {
                parsedPresets.push(self.parsePreset(preset));
            });
            if (sortFunction !== null){
                parsedPresets = parsedPresets.sort(sortFunction);
            }
            return parsedPresets
        }
    }
    return new BackendService();
});