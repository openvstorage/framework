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
    'jquery', 'knockout', 'ovs/shared', './data', 'ovs/api', 'ovs/generic', '../../containers/storagerouter'
], function ($, ko, shared, data, api, generic, StorageRouter) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data   = data;
        self.shared = shared;

        // Handles
        self.loadSRMetadataHandle     = undefined;
        self.loadStorageRoutersHandle = undefined;

        // Observables
        self.preValidateResult = ko.observable({ valid: true, reasons: [], fields: [] });

        // Computed
        self.canContinue = ko.computed(function () {
            var valid = true, showErrors = false, reasons = [], fields = [], preValidation = self.preValidateResult();
            if (preValidation.valid === false) {
                showErrors = true;
                reasons = reasons.concat(preValidation.reasons);
                fields = fields.concat(preValidation.fields);
            }
            return { value: valid, showErrors: showErrors, reasons: reasons, fields: fields };
        });
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

        // Functions
        self.next = function() {
            return true;
        };

        // Durandal
        self.activate = function() {
            self.loadStorageRoutersHandle = api.get('storagerouters', { queryparams: { contents: 'storagedrivers' }})
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
            $.each(self.data.storageRouters(), function(index, storageRouter) {
                if (storageRouter === self.data.target()) {
                    $.each(self.data.dtlTransportModes(), function (i, key) {
                        if (key.name === 'rdma') {
                            self.data.dtlTransportModes()[i].disabled = storageRouter.rdmaCapable() === undefined ? true : !storageRouter.rdmaCapable();
                            return false;
                        }
                    });
                }
            });
        };
        self.preValidate = function() {
            var validationResult = { valid: true, reasons: [], fields: [] };
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.loadSRMetadataHandle);
                self.loadSRMetadataHandle = api.post('storagerouters/' + self.data.target().guid() + '/get_metadata')
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
                        var readOverlap,
                            writeOverlap,
                            requiredRoles = ['READ', 'WRITE', 'DB'],
                            dbPartitionGuids = [],
                            readPartitionGuids = [],
                            writePartitionGuids = [];
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
                            validationResult.valid = false;
                            validationResult.reasons.push($.t('ovs:wizards.addvpool.gathervpool.missing_role', { what: role }));
                        });
                        if (self.data.backend() === 'distributed' && self.data.mountpoints().length === 0) {
                            validationResult.valid = false;
                            validationResult.reasons.push($.t('ovs:wizards.addvpool.gathervpool.missing_mountpoints'));
                        }
                        readOverlap = generic.overlap(dbPartitionGuids, readPartitionGuids);
                        writeOverlap = generic.overlap(dbPartitionGuids, writePartitionGuids);
                        if (readOverlap || writeOverlap) {
                            var write, max = 0, scoSize = self.data.scoSize() * 1024 * 1024,
                                fragSize = self.data.albaPreset().fragment_size,
                                totalSize = self.data.albaBackend().totalSize();
                            $.each(self.data.albaBackend().enhancedPresets()[0].policies, function(index, policy) {
                                var sizeToReserve = totalSize / scoSize * (1200 + (policy.k + policy.m) * (25 * scoSize / policy.k / fragSize + 56));
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
                            if (self.data.readCacheAvailableSize() + self.data.sharedSize() <= 10 * 1024 * 1024 * 1024) {
                                validationResult.valid = false;
                                validationResult.reasons.push($.t('ovs:wizards.addvpool.gathervpool.insufficient_readcache_size'));
                            }
                            if (self.data.writeCacheAvailableSize() + self.data.sharedSize() <= 10 * 1024 * 1024 * 1024) {
                                validationResult.valid = false;
                                validationResult.reasons.push($.t('ovs:wizards.addvpool.gathervpool.insufficient_writecache_size'));
                            }
                        }

                        self.data.readCacheSize(Math.floor(self.data.readCacheAvailableSize() / 1024 / 1024 / 1024));
                        if (self.data.readCacheAvailableSize() === 0) {
                            write = Math.floor((self.data.writeCacheAvailableSize() + self.data.sharedSize()) / 1024 / 1024 / 1024) - 1;
                        } else {
                            write = Math.floor((self.data.writeCacheAvailableSize() + self.data.sharedSize()) / 1024 / 1024 / 1024);
                        }
                        self.data.writeCacheSize(write);
                    })
                    .fail(deferred.reject)
                    .always(function() {
                        self.preValidateResult(validationResult);
                        if (validationResult.valid) {
                            deferred.resolve();
                        } else {
                            deferred.reject();
                        }
                    });
            }).promise();
        };
    };
});
