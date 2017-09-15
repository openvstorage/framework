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
define(['jquery', 'knockout', 'ovs/generic'], function($, ko, generic){
    "use strict";
    var singleton;

    singleton = function() {
        var wizardData = {
            storageRouter:           ko.observable(),
            storageDriver:           ko.observable(),
            vPool:                   ko.observable(),
            // General changes
            proxyAmount:             ko.observable(),
            // Fragment cache
            fragmentCache:           ko.observable(),
            useFragmentCache:        ko.observable(false),
            // Block cache
            blockCache:              ko.observable(),
            // Shared data across the pages

        };
        // Computed
        wizardData.enhancedPreset = ko.computed(function() {
            /**
             * Compute a preset to look like presetName: (1,1,1,1),(2,1,2,1)
             */
            var vpool = wizardData.vPool();
            var enhanchedPreset = undefined;
            if (vpool === undefined || (vpool.backendPolicies().length === 0 && vpool.backendPreset === undefined)) {
               return undefined
            }
            enhanchedPreset = vpool.backendPreset() +': ';
            $.each(vpool.backendPolicies(), function(index, policy) {
                if (index !== 0){
                    enhanchedPreset += ', '
                }
                enhanchedPreset += '(' + policy.join(', ') + ')'
            });
            return enhanchedPreset
        });

        // Functions
        wizardData.setFragmentCache = function(vpool) {

        };
        return wizardData;
    };
    return singleton();
});
