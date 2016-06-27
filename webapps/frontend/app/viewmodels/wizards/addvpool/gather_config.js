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
    'jquery', 'knockout', 'ovs/shared', './data', 'ovs/api', 'ovs/generic'
], function ($, ko, shared, data, api, generic) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data   = data;
        self.shared = shared;

        // Handles
        self.loadSRMetadataHandle = undefined;

        // Observables
        self.preValidateResult = ko.observable({ valid: true, reasons: [], fields: [] });
        self.initialGetMetadata = ko.observable(true);

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

        // Durandal
        self.activate = function() {
            // Reset pre-validation results
            self.validationResult = { valid: true, reasons: [], fields: [] };
            self.preValidateResult(self.validationResult);
        };
        self.preValidate = function() {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.loadSRMetadataHandle);
                self.loadSRMetadataHandle = api.post('storagerouters/' + self.data.storageRouter().guid() + '/get_metadata')
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
                        var dbOverlap,
                            readOverlap,
                            writeOverlap,
                            requiredRoles = ['READ', 'WRITE', 'DB'],
                            dbPartitionGuids = [],
                            readPartitionGuids = [],
                            writePartitionGuids = [],
                            nsmPartitionGuids = self.data.albaBackend() !== undefined ? self.data.albaBackend().metadata_information.nsm_partition_guids : [];
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
                            self.validationResult.valid = false;
                            self.validationResult.reasons.push($.t('ovs:wizards.add_vpool.gather_config.missing_role', { what: role }));
                        });
                        if (self.data.backend() === 'distributed' && self.data.mountpoints().length === 0) {
                            self.validationResult.valid = false;
                            self.validationResult.reasons.push($.t('ovs:wizards.add_vpool.gather_config.missing_mountpoints'));
                        }
                        if (self.data.scrubAvailable() === false) {
                            self.validationResult.valid = false;
                            self.validationResult.reasons.push($.t('ovs:wizards.add_vpool.gather_config.missing_role', { what: 'SCRUB' }));
                        }
                        dbOverlap = generic.overlap(dbPartitionGuids, nsmPartitionGuids);
                        readOverlap = dbOverlap && generic.overlap(dbPartitionGuids, readPartitionGuids);
                        writeOverlap = dbOverlap && generic.overlap(dbPartitionGuids, writePartitionGuids);
                        if (readOverlap || writeOverlap) {
                            var write, max = 0, scoSize = self.data.scoSize() * 1024 * 1024,
                                fragSize = self.data.albaPreset().fragSize,
                                totalSize = self.data.albaBackend().ns_statistics.global.size;
                            $.each(self.data.albaPreset().policies, function(index, policy) {
                                // For more information about below formula: see http://jira.cloudfounders.com/browse/OVS-3553
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
                        }

                        if (self.initialGetMetadata() === true) {
                            self.data.readCacheSize(Math.floor(self.data.readCacheAvailableSize() / 1024 / 1024 / 1024));
                            if (self.data.readCacheAvailableSize() === 0) {
                                write = Math.floor((self.data.writeCacheAvailableSize() + self.data.sharedSize()) / 1024 / 1024 / 1024) - 1;
                            } else {
                                write = Math.floor((self.data.writeCacheAvailableSize() + self.data.sharedSize()) / 1024 / 1024 / 1024);
                            }
                            self.data.writeCacheSize(write);
                        }
                    })
                    .fail(deferred.reject)
                    .always(function() {
                        self.preValidateResult(self.validationResult);
                        if (self.validationResult.valid) {
                            deferred.resolve();
                        } else {
                            deferred.reject();
                        }
                        self.initialGetMetadata(false);
                    });
            }).promise();
        };
    };
});
