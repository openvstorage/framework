// Copyright 2014 iNuron NV
//
// Licensed under the Open vStorage Modified Apache License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define([
    'jquery', 'knockout', 'ovs/generic', './data', 'ovs/api', '../../containers/storagerouter',
    '../../containers/storagedriver', 'ovs/shared'
], function($, ko, generic, data, api, StorageRouter, StorageDriver, shared) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data = data;
        self.shared = shared;
        self.activateResult = { valid: true, reasons: [], fields: [] };

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
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.over_allocation'));
            }
            if (writeCacheSizeBytes > writeCacheSizeAvailableBytes) {
                fields.push('writeCacheSize');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.over_allocation'));
            }
            if (readCacheSizeBytes + writeCacheSizeBytes > self.data.readCacheAvailableSize() + self.data.writeCacheAvailableSize() + sharedAvailableModulus) {
                fields.push('readCacheSize');
                fields.push('writeCacheSize');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.over_allocation'));
            }
            if (self.data.scrubAvailable() === false) {
                reasons.push($.t('ovs:wizards.addvpool.gathervpool.missing_role', { what: 'SCRUB' }));
            }
            var valid = reasons.length === 0;
            if (self.activateResult.valid === false) {
                valid = false;
                fields.push.apply(fields, self.activateResult.fields);
                reasons.push.apply(reasons, self.activateResult.reasons);
            }
            var unique_fields = fields.filter(generic.arrayFilterUnique);
            var unique_reasons = reasons.filter(generic.arrayFilterUnique);
            return { value: valid, reasons: unique_reasons, fields: unique_fields };
        });

        self.activate = function() {
            if (data.extendVpool() === true) {
                self.loadStorageRoutersHandle = api.get('storagerouters', { queryparams: {
                        contents: 'storagedrivers',
                        sort: 'name'
                }})
                    .done(function(data) {
                        var guids = [], srdata = {};
                        $.each(data.data, function(index, item) {
                            guids.push(item.guid);
                            srdata[item.guid] = item;
                        });
                        generic.crossFiller(
                            guids, self.data.storageRouters,
                            function(guid) {
                                return new StorageRouter(guid);
                            }, 'guid'
                        );
                        $.each(self.data.storageRouters(), function(index, storageRouter) {
                            storageRouter.fillData(srdata[storageRouter.guid()]);
                        });
                    });
                if (self.data.target() === undefined && self.data.storageRouter() !== undefined) {
                    self.data.target(self.data.storageRouter);
                }
                if (data.vPool() !== undefined) {
                    self.data.vPool().load('storagedrivers', { skipDisks: true })
                        .then(function() {
                            self.data.storageDriver(new StorageDriver(self.data.vPool().storageDriverGuids()[0]));
                            self.data.storageDriver().load()
                                .then(function() {
                                    self.data.storageIP(self.data.storageDriver().storageIP())
                                });
                            self.data.vPool().backendType().load();
                            self.data.backend(self.data.vPool().backendType().code().toLowerCase());
                            self.data.name(self.data.vPool().name());
                        })
                }
                api.post('storagerouters/' + self.data.target().guid() + '/get_metadata')
                    .then(self.shared.tasks.wait)
                    .then(function(data) {
                        self.data.mountpoints(data.mountpoints);
                        self.data.partitions(data.partitions);
                        self.data.ipAddresses(data.ipaddresses);
                        self.data.sharedSize(data.shared_size);
                        self.data.scrubAvailable(data.scrub_available);
                        self.data.readCacheAvailableSize(data.readcache_size);
                        self.data.writeCacheAvailableSize(data.writecache_size);
                    })
                    .done(function() {
                        self.activateResult = { valid: true, reasons: [], fields: [] };
                        var dbOverlap,
                            readOverlap,
                            writeOverlap,
                            requiredRoles = ['READ', 'WRITE', 'DB'],
                            dbPartitionGuids = [],
                            readPartitionGuids = [],
                            writePartitionGuids = [],
                            nsmPartitionGuids = data.vPool().metadata().backend_info.nsm_partition_guids;
                        $.each(self.data.partitions(), function(role, partitions) {
                            if (requiredRoles.contains(role) && partitions.length > 0) {
                                generic.removeElement(requiredRoles, role);
                            }
                            $.each(partitions, function(index, partition) {
                                if (role === 'READ') {
                                    readPartitionGuids.push(partition.guid);
                                } else if (role === 'WRITE') {
                                    writePartitionGuids.push(partition.guid);
                                } else if (role === 'DB') {
                                    dbPartitionGuids.push(partition.guid);
                                }
                            });
                        });

                        $.each(requiredRoles, function(index, role) {
                            self.activateResult.valid = false;
                            self.activateResult.reasons.push($.t('ovs:wizards.addvpool.gathervpool.missing_role', { what: role }));
                        });
                        if (self.data.backend() === 'distributed' && self.data.mountpoints().length === 0) {
                            self.activateResult.valid = false;
                            self.activateResult.reasons.push($.t('ovs:wizards.addvpool.gathervpool.missing_mountpoints'));
                        }

                        dbOverlap = generic.overlap(dbPartitionGuids, nsmPartitionGuids);
                        readOverlap = dbOverlap && generic.overlap(dbPartitionGuids, readPartitionGuids);
                        writeOverlap = dbOverlap && generic.overlap(dbPartitionGuids, writePartitionGuids);
                        if (readOverlap || writeOverlap) {
                            var write, max = 0,
                                policies = data.vPool().metadata().backend_info.policies,
                                scoSize = data.vPool().metadata().backend_info.sco_size,
                                fragSize = data.vPool().metadata().backend_info.frag_size,
                                totalSize = data.vPool().metadata().backend_info.total_size;
                            $.each(policies, function(index, policy) {
                                // For more information about below formula: see http://jira.cloudfounders.com/browse/OVS-3553
                                var sizeToReserve = totalSize / scoSize * (1200 + (policy[0] + policy[1]) * (25 * scoSize / policy[0] / fragSize + 56));
                                if (sizeToReserve > max) {
                                    max = sizeToReserve;
                                }
                            });
                            if (readOverlap && writeOverlap) { // Only 1 DB role possible ==> READ and WRITE must be shared
                                self.data.sharedSize(self.data.sharedSize() - max);
                                if (self.data.sharedSize() < 0) {
                                    self.data.sharedSize(0);
                                }
                            } else if (readOverlap) {
                                self.data.readCacheAvailableSize(self.data.readCacheAvailableSize() - max);
                                if (self.data.readCacheAvailableSize() < 0) {
                                    self.data.readCacheAvailableSize(0);
                                }
                            } else if (writeOverlap) {
                                self.data.writeCacheAvailableSize(self.data.writeCacheAvailableSize() - max);
                                if (self.data.writeCacheAvailableSize() < 0) {
                                    self.data.writeCacheAvailableSize(0);
                                }
                            }
                        }
                        if (self.data.readCacheAvailableSize() + self.data.sharedSize() <= 10 * 1024 * 1024 * 1024) {
                            self.activateResult.valid = false;
                            self.activateResult.reasons.push($.t('ovs:wizards.addvpool.gathervpool.insufficient_space_left', { what: 'READ' }));
                        }
                        if (self.data.writeCacheAvailableSize() + self.data.sharedSize() <= 10 * 1024 * 1024 * 1024) {
                            self.activateResult.valid = false;
                            self.activateResult.reasons.push($.t('ovs:wizards.addvpool.gathervpool.insufficient_space_left', { what: 'WRITE' }));
                        }

                        self.data.readCacheSize(Math.floor(self.data.readCacheAvailableSize() / 1024 / 1024 / 1024));
                        if (self.data.readCacheAvailableSize() === 0) {
                            write = Math.floor((self.data.writeCacheAvailableSize() + self.data.sharedSize()) / 1024 / 1024 / 1024) - 1;
                        } else {
                            write = Math.floor((self.data.writeCacheAvailableSize() + self.data.sharedSize()) / 1024 / 1024 / 1024);
                        }
                        self.data.writeCacheSize(write);
                    })
            }
        }
    };
});
