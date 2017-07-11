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
define(['jquery', 'knockout'], function($, ko){
    "use strict";
    var hostRegex, nameRegex, parsePresets, singleton;
    hostRegex = /^((((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))|((([a-z0-9]+[\.\-])*[a-z0-9]+\.)+[a-z]{2,4}))$/;
    nameRegex = /^[0-9a-z][\-a-z0-9]{1,18}[a-z0-9]$/;

    parsePresets = function(backend) {
        var presets = [], policies, newPolicy, isAvailable, isActive, inUse,
            policyMapping = ['grey', 'black', 'green'], worstPolicy, replication, policyObject;
        $.each(backend.presets, function(index, preset) {
            worstPolicy = 0;
            policies = [];
            replication = undefined;
            $.each(preset.policies, function(jndex, policy) {
                policyObject = JSON.parse(policy.replace('(', '[').replace(')', ']'));
                isAvailable = preset.policy_metadata[policy].is_available;
                isActive = preset.policy_metadata[policy].is_active;
                inUse = preset.policy_metadata[policy].in_use;
                newPolicy = {
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
            presets.push({
                policies: policies,
                name: preset.name,
                compression: preset.compression,
                fragSize: preset.fragment_size,
                encryption: preset.fragment_encryption,
                color: policyMapping[worstPolicy],
                inUse: preset.in_use,
                isDefault: preset.is_default,
                replication: replication
            });
        });
        return presets.sort(function(preset1, preset2) {
            return preset1.name.toLowerCase() < preset2.name.toLowerCase() ? -1 : 1;
        });
    };

    singleton = function() {
        var wizardData = {
            hprmPort:                ko.observable().extend({ numeric: {min: 1, max: 65535}}),
            identifier:              ko.observable('').extend({regex: nameRegex}),
            storageRouter:           ko.observable(),
            vPool:                   ko.observable(),
            // Required for vPool cache pages that are used in this wizard
            backend:                 ko.observable(),
            backends:                ko.observableArray([]),
            localHost:               ko.observable(false),
            storageRoutersUsed:      ko.observableArray([]),
            vPoolAdd:                ko.observable(true),
            // Fragment cache
            backendFC:               ko.observable(),
            cacheQuotaFC:            ko.observable(),
            cacheQuotaFCConfigured:  ko.observable(false),
            clientIDFC:              ko.observable('').extend({removeWhiteSpaces: null}),
            clientSecretFC:          ko.observable('').extend({removeWhiteSpaces: null}),
            fragmentCacheOnRead:     ko.observable(true),
            fragmentCacheOnWrite:    ko.observable(true),
            hostFC:                  ko.observable('').extend({regex: hostRegex}),
            localHostFC:             ko.observable(true),
            localPathFC:             ko.observable(''),
            localSizeFC:             ko.observable().extend({ numeric: {min: 1, max: 10 * 1024}}),
            portFC:                  ko.observable(80).extend({numeric: {min: 1, max: 65536}}),
            presetFC:                ko.observable(),
            useFC:                   ko.observable(false),
            // Block cache
            backendBC:               ko.observable(),
            blockCacheOnRead:        ko.observable(true),
            blockCacheOnWrite:       ko.observable(true),
            cacheQuotaBC:            ko.observable(),
            cacheQuotaBCConfigured:  ko.observable(false),
            clientIDBC:              ko.observable('').extend({removeWhiteSpaces: null}),
            clientSecretBC:          ko.observable('').extend({removeWhiteSpaces: null}),
            hostBC:                  ko.observable('').extend({regex: hostRegex}),
            localHostBC:             ko.observable(true),
            localPathBC:             ko.observable(''),
            localSizeBC:             ko.observable().extend({ numeric: {min: 1, max: 10 * 1024}}),
            portBC:                  ko.observable(80).extend({numeric: {min: 1, max: 65536}}),
            presetBC:                ko.observable(),
            supportsBC:              ko.observable(true),
            useBC:                   ko.observable(false)
        };
        wizardData.enhancedPresetsFC = ko.computed(function(){
            if (wizardData.backendFC() === undefined){
                wizardData.presetFC(undefined);
                return []
            }
            var presets = parsePresets(wizardData.backendFC()),
                presetNames = [];
            $.each(wizardData.backendFC().presets, function(_, preset) {
                presetNames.push(preset.name);
            });
            if (wizardData.presetFC() === undefined) {
                wizardData.presetFC(presets[0]);
            } else if (!presetNames.contains(wizardData.presetFC().name)) {
                wizardData.presetFC(presets[0]);
            }
            return presets;
        });
        wizardData.enhancedPresetsBC = ko.computed(function(){
            if (wizardData.backendBC() === undefined){
                wizardData.presetBC(undefined);
                return []
            }
            var presets = parsePresets(wizardData.backendBC()),
                presetNames = [];
            $.each(wizardData.backendBC().presets, function(_, preset) {
                presetNames.push(preset.name);
            });
            if (wizardData.presetBC() === undefined) {
                wizardData.presetBC(presets[0]);
            } else if (!presetNames.contains(wizardData.presetBC().name)) {
                wizardData.presetBC(presets[0]);
            }
            return presets;
        });
        return wizardData;
    };
    return singleton();
});
