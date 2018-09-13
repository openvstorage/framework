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
    './data',
    'ovs/api', 'ovs/generic', 'ovs/shared',
    'viewmodels/services/storagedriver'
], function($, ko, data, api, generic, shared,
            StoragedriverService) {
    "use strict";
    return function(stepOptions) {
        var self = this;

        // Variables
        self.activated = false;
        self.data   = stepOptions.data;
        self.shared = shared;

        // Observables
        self.loadingUpdateImpact = ko.observable(false);
        // Computed
        self.canContinue = ko.pureComputed(function() {
            return { value: true, reasons: [], fields: [] };
        });
        self.canShowFragmentCacheQuota = ko.pureComputed(function() {
            return self.data.cachingData.fragment_cache.isUsed() && ['', undefined].contains(self.data.cachingData.block_cache.quota())
        });
        self.canShowBlockCacheQuota = ko.pureComputed(function() {
            return self.data.cachingData.block_cache.isUsed() && ['', undefined].contains(self.data.cachingData.block_cache.quota())
        });


        // Functions
        self.formatFloat = function(value) {
            return parseFloat(value);
        };
        self.calculateUpdateImpact = function () {
            var postData = {
                vpool_updates: ko.mapping.toJS(self.data.configParams),
                storagedriver_updates: ko.mapping.toJS(self.data.cachingData)
            };
            postData.storagedriver_updates.proxy_amount = self.data.storageDriverParams.proxyAmount();
            postData.storagedriver_updates.global_write_buffer = self.data.storageDriverParams.globalWriteBuffer();
            self.loadingUpdateImpact(true);
            return StoragedriverService.calculateUpdateImpact(self.data.storageDriver.guid(), postData).then(function(result) {
                console.log(result);
                return result
            }).always(function() {
                self.loadingUpdateImpact(false);
            })
        };
        self.finish = function() {
            return $.when()
                .then(function() {
            })
        };

        // Durandal
        self.activate = function() {
            if (self.activated === true){
                return
            }
            self.calculateUpdateImpact()
            self.activated = true;
        }
    };
});
