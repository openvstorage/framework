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
    'jquery', 'knockout', './data'
], function($, ko, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data = data;

        // Computed
        self.canContinue = ko.computed(function() {
            var reasons = [], fields = [];
            var readCacheSizeBytes = self.data.readCacheSize() * 1024 * 1024 * 1024;
            var writeCacheSizeBytes = self.data.writeCacheSize() * 1024 * 1024 * 1024;
            var readCacheSizeAvailableBytes = self.data.readCacheAvailableSize() + self.data.sharedSize();
            var writeCacheSizeAvailableBytes = self.data.writeCacheAvailableSize() + self.data.sharedSize();
            var sharedAvailableModulus = self.data.sharedSize() - self.data.sharedSize() % (1024 * 1024 * 1024);
            if (readCacheSizeBytes > readCacheSizeAvailableBytes) {
                fields.push('readCacheSize');
                reasons.push($.t('ovs:wizards.add_vpool.gather_cache_info.over_allocation'));
            }
            else if (writeCacheSizeBytes > writeCacheSizeAvailableBytes) {
                fields.push('writeCacheSize');
                reasons.push($.t('ovs:wizards.add_vpool.gather_cache_info.over_allocation'));
            }
            else if (readCacheSizeBytes + writeCacheSizeBytes > self.data.readCacheAvailableSize() + self.data.writeCacheAvailableSize() + sharedAvailableModulus) {
                fields.push('readCacheSize');
                fields.push('writeCacheSize');
                reasons.push($.t('ovs:wizards.add_vpool.gather_cache_info.over_allocation'));
            }
            return { value: reasons.length === 0, reasons: reasons, fields: fields };
        });
    };
});
