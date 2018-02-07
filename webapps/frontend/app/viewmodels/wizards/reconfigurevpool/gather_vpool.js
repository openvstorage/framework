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
    'ovs/shared', 'ovs/api', 'ovs/generic',
    'viewmodels/containers/storagerouter/storagerouter', 'viewmodels/containers/storagedriver/storagedriver',
    'viewmodels/containers/vpool/vpool',
    './data'
], function ($, ko, shared, api, generic, StorageRouter, StorageDriver, VPool, data) {
    "use strict";
    return function () {
        var self = this;

        // Variables
        self.data   = data;
        self.shared = shared;

        // Computed
        self.canContinue = ko.computed(function () {
            var reasons = [], fields = [];
            return { value: reasons.length === 0, reasons: reasons, fields: fields };
        });
        self.enhancedPreset = ko.computed(function() {
            /**
             * Compute a preset to look like presetName: (1,1,1,1),(2,1,2,1)
             */
            var vpool = self.data.vPool();
            if (vpool === undefined || (vpool.backendPolicies().length === 0 && vpool.backendPreset === undefined)) {
               return undefined
            }
            var policies = [];
            $.each(vpool.backendPolicies(), function(index, policy) {
                policies.push('(' + policy.join(', ') + ')')
            });
            return vpool.backendPreset() +': ' + policies.join(', ')
        });


    };
});
