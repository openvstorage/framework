// Copyright 2016 iNuron NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
