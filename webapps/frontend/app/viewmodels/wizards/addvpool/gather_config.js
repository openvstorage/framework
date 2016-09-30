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
    'jquery', 'knockout', 'ovs/generic', './data'
], function ($, ko, generic, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data = data;

        // Computed
        self.dtlMode = ko.computed({
            write: function(mode) {
                if (mode.name === 'no_sync') {
                    self.data.dtlEnabled(false);
                } else {
                    self.data.dtlEnabled(true);
                }
                self.data.dtlMode(mode);
            },
            read: function() {
                if (self.data.vPoolAdd() === false && self.data.dtlEnabled() === false) {
                    return {name: 'no_sync', disabled: false};
                }
                return self.data.dtlMode();
            }
        });
        self.controlledWriteCacheSize = ko.computed({
            read: function() {
                if (self.data.writeCacheSize() === undefined) {
                    var shared = self.data.sharedSize();
                    if (self.data.readCacheAvailableSize() === 0 && self.data.cacheStrategy() !== 'none') {
                        shared = Math.floor(self.data.sharedSize() / 2);
                    }
                    self.data.writeCacheSize(Math.floor((self.data.writeCacheAvailableSize() + shared) / 1024 / 1024 / 1024));
                }
                return self.data.writeCacheSize();
            },
            write: function(size) {
                self.data.writeCacheSize(size);
            }
        }).extend({ notify: 'always' });
        self.controlledReadCacheSize = ko.computed({
            read: function() {
                if (self.data.cacheStrategy() === 'none') {
                    self.data.readCacheSize(undefined);
                } else if (self.data.readCacheSize() === undefined) {
                    var read = self.data.readCacheAvailableSize();
                    if (read === 0) {
                        self.data.readCacheSize(Math.floor(Math.floor(self.data.sharedSize() / 2) / 1024 / 1024 / 1024));
                    } else {
                        self.data.readCacheSize(Math.floor(read / 1024 / 1024 / 1024));
                    }
                }
                return self.data.readCacheSize();
            },
            write: function(size) {
                self.data.readCacheSize(size);
            }
        }).extend({ notify: 'always' });
        self.canContinue = ko.computed(function () {
            var reasons = [], fields = [], roleFound = false,
                readCacheSizeBytes = self.data.readCacheSize() * 1024 * 1024 * 1024,
                writeCacheSizeBytes = self.data.writeCacheSize() * 1024 * 1024 * 1024,
                readCacheSizeAvailableBytes = self.data.readCacheAvailableSize() + self.data.sharedSize(),
                writeCacheSizeAvailableBytes = self.data.writeCacheAvailableSize() + self.data.sharedSize(),
                sharedAvailableModulus = self.data.sharedSize() - self.data.sharedSize() % (1024 * 1024 * 1024);
            if (self.data.cacheStrategy() === 'none') {
                readCacheSizeBytes = 0;
            }
            if (self.data.cacheStrategy() !== 'none' && self.data.partitions() !== undefined) {
                $.each(self.data.partitions(), function (role, partitions) {
                    if (role === 'READ' && partitions.length > 0) {
                        roleFound = true;
                    }
                });
                if (roleFound === false) {
                    reasons.push($.t('ovs:wizards.add_vpool.gather_config.missing_role', { what: 'READ' }));
                }
            }
            if (readCacheSizeBytes > readCacheSizeAvailableBytes) {
                fields.push('readCacheSize');
                reasons.push($.t('ovs:wizards.add_vpool.gather_config.over_allocation'));
            }
            if (writeCacheSizeBytes > writeCacheSizeAvailableBytes) {
                fields.push('writeCacheSize');
                reasons.push($.t('ovs:wizards.add_vpool.gather_config.over_allocation'));
            }
            if (readCacheSizeBytes + writeCacheSizeBytes > self.data.readCacheAvailableSize() + self.data.writeCacheAvailableSize() + sharedAvailableModulus) {
                fields.push('readCacheSize');
                fields.push('writeCacheSize');
                reasons.push($.t('ovs:wizards.add_vpool.gather_config.over_allocation'));
            }
            var uniqueReasons = [];
            $.each(reasons, function(_, reason) {
                if (!uniqueReasons.contains(reason)) {
                    uniqueReasons.push(reason);
                }
            });
            return { value: reasons.length === 0, reasons: uniqueReasons, fields: fields };
        });
    };
});
