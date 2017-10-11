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
    /**
     * Compute a preset to look like presetName: (1,1,1,1),(2,1,2,1)
     * @param presetName: Name of the preset
     * @param policies: Array of policies (eg.[[5, 4, 8, 3], [2, 2, 3, 4]])
     */
    function enhancePreset(presetName, policies){
        var convertedPolicies = [];
        $.each(policies, function(index, policy) {
            convertedPolicies.push('(' + policy.join(', ') + ')')
        });
        return presetName +': ' + convertedPolicies.join(', ')
    }
    /**
     * Loads in all backends for the current supplied data
     * @param queryParams: Additional query params. Defaults to no params
     * @param relay: Relay to use (Optional, defaults to no relay)
     * @returns {Deferred}
    */
    function loadAlbaBackends(queryParams, relay) {
        relay = (typeof sortFunction !== 'undefined') ? relay: '';
        queryParams = (typeof queryParams !== 'undefined') ? queryParams : {};
        return api.get(relay + 'alba/backends', { queryparams: queryParams })
    }
    /**
     * Loads in a backend for the current supplied data
     * @param guid: Guid of the Alba Backend
     * @param queryParams: Additional query params. Defaults to no params
     * @param relay: Relay to use (Optional, defaults to no relay)
     * @returns {Deferred}
    */
    function loadAlbaBackend(guid, queryParams, relay) {
         relay = (typeof sortFunction !== 'undefined') ? relay: '';
        queryParams = (typeof queryParams !== 'undefined') ? queryParams : {};
        return api.get(relay + 'alba/backends/' + guid + '/', { queryparams: queryParams });
    }
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
    function parsePreset(preset){
        var worstPolicy = 0;
        var policies = [];
        var replication = undefined;
        var policyObject = undefined;
        var policyMapping = ['grey', 'black', 'green'];
        $.each(preset.policies, function (jndex, policy) {
            policyObject = JSON.parse(policy.replace('(', '[').replace(')', ']'));
            var isAvailable = preset.policy_metadata[policy].is_available;
            var isActive = preset.policy_metadata[policy].is_active;
            var inUse = preset.policy_metadata[policy].in_use;
            var newPolicy = {
                text: policy,
                color: 'grey',
                isActive: false,
                k: policyObject[0],
                m: policyObject[1],
                c: policyObject[2],
                x: policyObject[3]
            };
            if (isAvailable) {
                newPolicy.color = 'black';
            }
            if (isActive) {
                newPolicy.isActive = true;
            }
            if (inUse) {
                newPolicy.color = 'green';
            }
            worstPolicy = Math.max(policyMapping.indexOf(newPolicy.color), worstPolicy);
            policies.push(newPolicy);
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
            color: policyMapping[worstPolicy],
            inUse: preset.in_use,
            isDefault: preset.is_default,
            replication: replication
        }
    }
    /**
     * Parse multiple presents and sorts
     * @param presets: Array of presets as returned by the api
     * @param sortFunction: Custom sort function to sort the array with (Optional, sorts by name by default, provide null to stop sorting)
     * @return {Array}
     */
    function parsePresets(presets, sortFunction) {
        if (typeof sortFunction === 'undefined') {
            sortFunction = function (preset1, preset2) {
                return preset1.name.toLowerCase() < preset2.name.toLowerCase() ? -1 : 1;
            }
        } else if (!generic.isFunction(sortFunction) && sortFunction !== null) {
            throw new Error('Provided sort function is not a function')
        }

        var parsedPresets = [];
        $.each(presets, function (index, preset) {
            parsedPresets.push(wizardData.parsePreset(preset));
        });
        if (sortFunction !== null){
            parsedPresets = parsedPresets.sort(sortFunction);
        }
        return parsedPresets
    }
    return {
        enhancePreset: enhancePreset,
        loadAlbaBackend: loadAlbaBackend,
        loadAlbaBackends: loadAlbaBackends,
        parsePreset: parsePreset,
        parsePresets: parsePresets
    }

});