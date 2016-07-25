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
                return self.data.dtlMode();
            }
        });
        self.overlaps = ko.computed(function() {
            var dbOverlap, readOverlap, writeOverlap, dbPartitionGuids = [], writePartitionGuids = [], readPartitionGuids = [],
                nsmPartitionGuids = self.data.albaBackend() !== undefined ? self.data.albaBackend().metadata_information.nsm_partition_guids : [];
            if (self.data.partitions() !== undefined) {
                $.each(self.data.partitions(), function (role, partitions) {
                    $.each(partitions, function (index, partition) {
                        if (role === 'READ') {
                            readPartitionGuids.push(partition.guid);
                        } else if (role === 'WRITE') {
                            writePartitionGuids.push(partition.guid);
                        } else if (role === 'DB') {
                            dbPartitionGuids.push(partition.guid);
                        }
                    });
                });
            }
            dbOverlap = generic.overlap(dbPartitionGuids, nsmPartitionGuids);
            readOverlap = dbOverlap && generic.overlap(dbPartitionGuids, readPartitionGuids);
            writeOverlap = dbOverlap && generic.overlap(dbPartitionGuids, writePartitionGuids);
            return {
                db: dbOverlap,
                read: readOverlap,
                write: writeOverlap
            };
        });
        self.nsmReserved = ko.computed(function() {
            if (self.data.albaPreset() === undefined || self.data.albaBackend() === undefined) {
                return 0;
            }
            var overlap = self.overlaps();
            if (overlap.read || overlap.write) {
                var max = 0, scoSize = self.data.scoSize() * 1024 * 1024,
                    fragSize = self.data.albaPreset().fragSize,
                    totalSize = self.data.albaBackend().usages.size;
                $.each(self.data.albaPreset().policies, function (index, policy) {
                    // For more information about below formula: see http://jira.cloudfounders.com/browse/OVS-3553
                    var sizeToReserve = totalSize / scoSize * (1200 + (policy.k + policy.m) * (25 * scoSize / policy.k / fragSize + 56));
                    if (sizeToReserve > max) {
                        max = sizeToReserve;
                    }
                });
                return max;
            }
            return 0;
        });
        self.correctedSharedSize = ko.computed(function() {
            var overlap = self.overlaps(), max = self.nsmReserved(), size = self.data.sharedSize();
            if (overlap.read && overlap.write) {
                size = Math.max(0, size - max);
            }
            return size;
        });
        self.correctedReadCacheAvailableSize = ko.computed(function() {
            var overlap = self.overlaps(), max = self.nsmReserved(), size = self.data.readCacheAvailableSize();
            if (overlap.read && overlap.write) {
                size = Math.max(0, size - max);
            }
            return size;
        });
        self.correctedWriteCacheAvailableSize = ko.computed(function() {
            var overlap = self.overlaps(), max = self.nsmReserved(), size = self.data.writeCacheAvailableSize();
            if (overlap.read && overlap.write) {
                size = Math.max(0, size - max);
            }
            return size;
        });
        self.controledWriteCacheSize = ko.computed({
            read: function() {
                if (self.data.writeCacheSize() === undefined) {
                    var shared = self.correctedSharedSize();
                    if (self.correctedReadCacheAvailableSize() === 0 && self.data.cacheStrategy() !== 'none') {
                        shared = Math.floor(self.correctedSharedSize() / 2);
                    }
                    self.data.writeCacheSize(Math.floor((self.correctedWriteCacheAvailableSize() + shared) / 1024 / 1024 / 1024));
                }
                return self.data.writeCacheSize();
            },
            write: function(size) {
                self.data.writeCacheSize(size);
            }
        }).extend({ notify: 'always' });
        self.controledReadCacheSize = ko.computed({
            read: function() {
                if (self.data.cacheStrategy() === 'none') {
                    self.data.readCacheSize(undefined);
                } else if (self.data.readCacheSize() === undefined) {
                    var read = self.correctedReadCacheAvailableSize();
                    if (read === 0) {
                        self.data.readCacheSize(Math.floor(Math.floor(self.correctedSharedSize() / 2) / 1024 / 1024 / 1024));
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
                readCacheSizeAvailableBytes = self.correctedReadCacheAvailableSize() + self.correctedSharedSize(),
                writeCacheSizeAvailableBytes = self.correctedWriteCacheAvailableSize() + self.correctedSharedSize(),
                sharedAvailableModulus = self.correctedSharedSize() - self.correctedSharedSize() % (1024 * 1024 * 1024);
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
            else if (writeCacheSizeBytes > writeCacheSizeAvailableBytes) {
                fields.push('writeCacheSize');
                reasons.push($.t('ovs:wizards.add_vpool.gather_config.over_allocation'));
            }
            else if (readCacheSizeBytes + writeCacheSizeBytes > self.correctedReadCacheAvailableSize() + self.correctedWriteCacheAvailableSize() + sharedAvailableModulus) {
                fields.push('readCacheSize');
                fields.push('writeCacheSize');
                reasons.push($.t('ovs:wizards.add_vpool.gather_config.over_allocation'));
            }
            return { value: reasons.length === 0, reasons: reasons, fields: fields };
        });
    };
});
