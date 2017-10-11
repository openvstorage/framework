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
    'viewmodels/services/backend',
    './data'
], function ($, ko, shared, api, generic, StorageRouter, StorageDriver, VPool, backendService, data) {
    "use strict";
    return function () {
        var self = this;

        // Variables
        self.data   = data;
        self.shared = shared;

        // Observables
        self._storageRouterIpAddresses = ko.observableArray([]);
        // Computed
        self.canContinue = ko.pureComputed(function () {
            var reasons = [], fields = [];
            return { value: reasons.length === 0, reasons: reasons, fields: fields };
        });
        self.storageRouterIpAddresses = ko.computed(function() {
            var ipAddresses = [];
            if (self.data.storageRouter() === undefined) {
                return ipAddresses
            } else {
                try{
                    var metadata = self.data.getStorageRouterMetadata(self.data.storageRouter().guid()).metadata;
                    ipAddresses = metadata.ipaddresses;
                } catch (error) {
                    if (error.code !== 'str_not_found') {
                        throw error // Throw it again
                    }
                    // Nothing found, return the empty array
                    return ipAddresses
                }
            }
            self._storageRouterIpAddresses(ipAddresses);
            console.log(ipAddresses)
            return ipAddresses;
        });
        self.storageRouterIpAddress = ko.computed({
            deferEvaluation: true,  // Wait with computing for an actual subscription
            read: function() {
                // Computed as the change of ip adresses should be accounted for
                var ipAddresses = self._storageRouterIpAddresses();
                var currentIP = self.data.storageDriverParams.storageIP();
                if (!ipAddresses.contains(currentIP)) {
                    // Select the first in the list (if possible)
                    if (ipAddresses.length > 0 ) {
                        currentIP = ipAddresses[0];
                    } else {
                        currentIP = undefined;
                    }
                    // Change our mapped property
                    self.data.storageDriverParams.storageIP(currentIP);
                }
                return currentIP;
            },
            write: function(ip) {
                self.storageRouterIpAddress(ip)
            }
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


    };
});
