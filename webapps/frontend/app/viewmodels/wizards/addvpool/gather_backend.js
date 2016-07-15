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
    'ovs/api', 'ovs/generic', 'ovs/shared',
    './data'
], function($, ko, api, generic, shared, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data   = data;
        self.shared = shared;

        // Handles
        self.fetchAlbaVPoolHandle = undefined;
        self.loadSRMetadataHandle = undefined;

        // Observables
        self.albaBackendLoading    = ko.observable(false);
        self.albaPresetMap         = ko.observable({});
        self.initialGetMetadata    = ko.observable(true);
        self.invalidAlbaInfo       = ko.observable(false);
        self.fragmentCacheSettings = ko.observableArray(['write', 'read', 'rw', 'none']);

        // Computed
        self.localBackendsAvailable = ko.computed(function() {
            if (self.data.localHost() && self.data.albaBackends().length < 2) {
                if (self.data.vPoolAdd()) {
                    self.data.aaLocalHost(false);
                }
                return false;
            }
            if (self.data.vPoolAdd()) {
                self.data.aaLocalHost(true);
            }
            return true;
        });
        self.isPresetAvailable = ko.computed(function() {
            var presetAvailable = true;
            if (self.data.albaAABackend() !== undefined && self.data.albaAAPreset() !== undefined) {
                var guid = self.data.albaAABackend().guid,
                    name = self.data.albaAAPreset().name;
                if (self.albaPresetMap().hasOwnProperty(guid) && self.albaPresetMap()[guid].hasOwnProperty(name)) {
                    presetAvailable = self.albaPresetMap()[guid][name];
                }
            }
            return presetAvailable;
        });
        self.reUseableStorageRouters = ko.computed(function() {
            var temp = self.data.storageRoutersUsed().slice();  // Make deep copy of the list
            temp.unshift(undefined);  // Insert undefined as element 0
            return temp;
        });
        self.canContinue = ko.computed(function() {
            var showErrors = false, reasons = [], fields = [];
            if (self.data.backend() === 'alba') {
                if (self.data.albaAABackend() === undefined && self.data.useAA()) {
                    reasons.push($.t('ovs:wizards.add_vpool.gather_backend.choose_backend'));
                    fields.push('backend');
                }
                if (!self.data.aaLocalHost()) {
                    if (!self.data.aaHost.valid()) {
                        fields.push('host');
                        reasons.push($.t('ovs:wizards.add_vpool.gather_backend.invalid_host'));
                    }
                    if (self.data.aaAccesskey() === '' || self.data.aaSecretkey() === '') {
                        fields.push('clientid');
                        fields.push('clientsecret');
                        reasons.push($.t('ovs:wizards.add_vpool.gather_backend.no_credentials'));
                    }
                    if (self.invalidAlbaInfo()) {
                        reasons.push($.t('ovs:wizards.add_vpool.gather_backend.invalid_alba_info'));
                        fields.push('clientid');
                        fields.push('clientsecret');
                        fields.push('host');
                    }
                }
            }
            var dbOverlap, readOverlap, writeOverlap, dbPartitionGuids = [], readPartitionGuids = [], writePartitionGuids = [],
                nsmPartitionGuids = self.data.albaBackend() !== undefined ? self.data.albaBackend().metadata_information.nsm_partition_guids : [],
                requiredRoles = ['WRITE', 'DB'];
            if (self.data.cacheStrategy() !== 'none') {
                requiredRoles.push('READ');
            }
            if (self.data.partitions() !== undefined) {
                $.each(self.data.partitions(), function (role, partitions) {
                    if (requiredRoles.contains(role) && partitions.length > 0) {
                        generic.removeElement(requiredRoles, role);
                    }
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
            $.each(requiredRoles, function(index, role) {
                reasons.push($.t('ovs:wizards.add_vpool.gather_backend.missing_role', { what: role }));
            });
            if (self.data.scrubAvailable() === false) {
                reasons.push($.t('ovs:wizards.add_vpool.gather_backend.missing_role', { what: 'SCRUB' }));
            }
            dbOverlap = generic.overlap(dbPartitionGuids, nsmPartitionGuids);
            readOverlap = dbOverlap && generic.overlap(dbPartitionGuids, readPartitionGuids);
            writeOverlap = dbOverlap && generic.overlap(dbPartitionGuids, writePartitionGuids);
            if (readOverlap || writeOverlap) {
                var write, max = 0, scoSize = self.data.scoSize() * 1024 * 1024,
                    fragSize = self.data.albaPreset().fragSize,
                    totalSize = self.data.albaBackend().usages.size;
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
            self.initialGetMetadata(false);

            return { value: reasons.length === 0, showErrors: showErrors, reasons: reasons, fields: fields };
        });
        self.fragmentCacheSetting = ko.computed({
            read: function() {
                if (self.data.fragmentCacheOnRead() && self.data.fragmentCacheOnWrite()) {
                    return 'rw';
                }
                if (self.data.fragmentCacheOnRead() || self.data.fragmentCacheOnWrite()) {
                    return self.data.fragmentCacheOnRead() ? 'read' : 'write';
                }
                return 'none';
            },
            write: function(cache) {
                self.data.fragmentCacheOnRead(['rw', 'read'].contains(cache));
                self.data.fragmentCacheOnWrite(['rw', 'write'].contains(cache));
                if (cache === 'none') {
                    self.data.cacheStrategy('on_read');
                    self.data.useAA(false);
                } else {
                    self.data.cacheStrategy('none');
                }
            }
        });

        self.shouldSkip = function() {
            return $.Deferred(function(deferred) {
                if (self.data.backend() === 'distributed') {
                    deferred.resolve(true);
                } else {
                    deferred.resolve(false);
                }
            }).promise();
        };

        // Functions
        self.loadAlbaBackends = function() {
            return $.Deferred(function(albaDeferred) {
                generic.xhrAbort(self.fetchAlbaVPoolHandle);
                var relay = '', remoteInfo = {},
                    getData = {
                        contents: 'available'
                    };
                if (!self.data.aaLocalHost()) {
                    relay = 'relay/';
                    remoteInfo.ip = self.data.aaHost();
                    remoteInfo.port = self.data.aaPort();
                    remoteInfo.client_id = self.data.aaAccesskey().replace(/\s+/, "");
                    remoteInfo.client_secret = self.data.aaSecretkey().replace(/\s+/, "");
                }
                $.extend(getData, remoteInfo);
                self.albaBackendLoading(true);
                self.invalidAlbaInfo(false);
                self.fetchAlbaVPoolHandle = api.get(relay + 'alba/backends', { queryparams: getData })
                    .done(function(data) {
                        var available_backends = [], calls = [];
                        $.each(data.data, function (index, item) {
                            if (item.available === true) {
                                getData.contents = 'metadata_information,name,ns_statistics,presets';
                                if (item.scaling === 'LOCAL') {
                                    getData.contents += ',asd_statistics';
                                }
                                calls.push(
                                    api.get(relay + 'alba/backends/' + item.guid + '/', { queryparams: getData })
                                        .then(function(data) {
                                            if (data.guid !== self.data.albaBackend().guid) {
                                                var asdsFound = false;
                                                if (data.scaling === 'LOCAL') {
                                                    $.each(data.asd_statistics, function(key, value) {  // As soon as we enter loop, we know at least 1 ASD is linked to this backend
                                                        asdsFound = true;
                                                        return false;
                                                    });
                                                }
                                                if (asdsFound === true || data.scaling === 'GLOBAL') {
                                                    available_backends.push(data);
                                                    self.albaPresetMap()[data.guid] = {};
                                                    $.each(data.presets, function (_, preset) {
                                                        self.albaPresetMap()[data.guid][preset.name] = preset.is_available;
                                                    });
                                                }
                                            }
                                        })
                                );
                            }
                        });
                        $.when.apply($, calls)
                            .then(function() {
                                if (available_backends.length > 0) {
                                    available_backends.sort(function(backend1, backend2) {
                                        return backend1.name.toLowerCase() < backend2.name.toLowerCase() ? -1 : 1;
                                    });
                                    self.data.albaAABackends(available_backends);
                                    self.data.albaAABackend(available_backends[0]);
                                    self.data.albaAAPreset(self.data.enhancedAAPresets()[0]);
                                } else {
                                    self.data.albaAABackends([]);
                                    self.data.albaAABackend(undefined);
                                    self.data.albaAAPreset(undefined);
                                }
                                self.albaBackendLoading(false);
                            })
                            .done(albaDeferred.resolve)
                            .fail(function() {
                                self.data.albaAABackends([]);
                                self.data.albaAABackend(undefined);
                                self.data.albaAAPreset(undefined);
                                self.albaBackendLoading(false);
                                self.invalidAlbaInfo(true);
                                albaDeferred.reject();
                            });
                    })
                    .fail(function() {
                        self.data.albaAABackends([]);
                        self.data.albaAABackend(undefined);
                        self.data.albaAAPreset(undefined);
                        self.albaBackendLoading(false);
                        self.invalidAlbaInfo(true);
                        albaDeferred.reject();
                    });
            }).promise();
        };

        // Durandal
        self.activate = function() {
            if (self.data.albaBackend() !== undefined && self.data.albaAABackend() !== undefined && self.data.albaBackend().guid === self.data.albaAABackend().guid) {
                self.data.albaAABackends([]);
                $.each(self.data.albaBackends(), function (_, backend) {
                    if (backend !== self.data.albaBackend() && !self.data.albaAABackends().contains(backend)) {
                        self.data.albaAABackends().push(backend);
                    }
                });
                if (self.data.albaAABackends().length === 0) {
                    self.data.albaAABackend(undefined);
                    self.data.albaAAPreset(undefined);
                } else {
                    self.data.albaAABackend(self.data.albaAABackends()[0]);
                    self.data.albaAAPreset(self.data.enhancedAAPresets()[0]);
                }
            }
        };
    };
});
