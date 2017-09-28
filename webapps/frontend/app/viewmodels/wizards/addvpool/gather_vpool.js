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
    '../../containers/storagerouter', '../../containers/storagedriver', '../../containers/vpool', './data'
], function ($, ko, shared, api, generic, StorageRouter, StorageDriver, VPool, data) {
    "use strict";
    return function () {
        var self = this;

        // Variables
        self.data   = data;
        self.shared = shared;

        // Handles
        self.checkS3Handle            = undefined;
        self.checkMtptHandle          = undefined;
        self.checkMetadataHandle      = undefined;
        self.fetchAlbaVPoolHandle     = undefined;
        self.loadSRMetadataHandle     = undefined;
        self.loadStorageRoutersHandle = undefined;

        // Observables
        self.albaPresetMap         = ko.observable({});
        self.invalidAlbaInfo       = ko.observable(false);
        self.ipAddresses           = ko.observableArray([]);
        self.loadingBackends       = ko.observable(false);
        self.loadingMetadata       = ko.observable(false);
        self.loadingPrevalidations = ko.observable(false);
        self.preValidateResult     = ko.observable({valid: true, reasons: [], fields: []});
        self.scrubAvailable        = ko.observable(false);
        self.srMetadataMap         = ko.observable({});

        // Computed
        self.canContinue = ko.computed(function () {
            var showErrors = false, reasons = [], fields = [], preValidation = self.preValidateResult(),
                requiredRoles = ['DB', 'DTL', 'WRITE'];
            if (self.data.vPool() === undefined) {
                if (!self.data.name.valid()) {
                    fields.push('name');
                    reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.invalid_name'));
                }
                else {
                    $.each(self.data.vPools(), function (index, vpool) {
                        if (vpool.name() === self.data.name()) {
                            fields.push('name');
                            reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.duplicate_name'));
                        }
                    });
                }
            }
            if (self.loadingPrevalidations() === true) {
                reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.validating_mountpoint'));
            } else {
                if (preValidation.valid === false) {
                    showErrors = true;
                    reasons = reasons.concat(preValidation.reasons);
                    fields = fields.concat(preValidation.fields);
                }
            }
            if (self.loadingBackends() === true) {
                reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.backends_loading'));
            } else {
                if (self.data.backend() === undefined) {
                    reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.choose_backend'));
                    fields.push('backend');
                } else if (self.data.preset() === undefined) {
                    reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.choose_preset'));
                    fields.push('preset');
                }
                if (self.invalidAlbaInfo() && !self.data.localHost()) {
                    reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.invalid_alba_info'));
                    fields.push('clientid');
                    fields.push('clientsecret');
                    fields.push('host');
                }
            }
            if (self.loadingMetadata() === true) {
                reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.metadata_loading'));
            } else {
                if (self.scrubAvailable() === false) {
                    reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.missing_role', {what: 'SCRUB'}));
                }
                if (self.data.partitions() !== undefined) {
                    $.each(self.data.partitions(), function (role, partitions) {
                        if (requiredRoles.contains(role) && partitions.length > 0) {
                            generic.removeElement(requiredRoles, role);
                        }
                    });
                    $.each(requiredRoles, function (index, role) {
                        reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.missing_role', {what: role}));
                    });
                }
                if (self.data.storageIP() === undefined) {
                    reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.missing_storage_ip'));
                    fields.push('storageip');
                }
            }
            if (!self.data.localHost()) {
                if (!self.data.host.valid()) {
                    fields.push('host');
                    reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.invalid_host'));
                }
                if (self.data.clientID() === '' || self.data.clientSecret() === '') {
                    fields.push('clientid');
                    fields.push('clientsecret');
                    reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.no_credentials'));
                }
                if (self.invalidAlbaInfo()) {
                    reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.invalid_alba_info'));
                    fields.push('clientid');
                    fields.push('clientsecret');
                    fields.push('host');
                }
            }
            return {value: reasons.length === 0, showErrors: showErrors, reasons: reasons, fields: fields};
        });
        self.isPresetAvailable = ko.computed(function () {
            var presetAvailable = true;
            if (self.data.backend() !== undefined && self.data.preset() !== undefined) {
                var guid = self.data.backend().guid,
                    name = self.data.preset().name;
                if (self.albaPresetMap().hasOwnProperty(guid) && self.albaPresetMap()[guid].hasOwnProperty(name)) {
                    presetAvailable = self.albaPresetMap()[guid][name];
                }
            }
            return presetAvailable;
        });

        // Functions
        self.resetBackends = function() {
            self.data.backends([]);
            self.data.backend(undefined);
            self.data.preset(undefined);
        };
        self.fillSRData = function (srData) {
            self.ipAddresses(srData.ipaddresses);
            self.data.partitions(srData.partitions);
            self.scrubAvailable(srData.scrub_available);
            if (srData.ipaddresses.length === 0) {
                self.data.storageIP(undefined);
            } else if (self.data.storageIP() === undefined || !srData.ipaddresses.contains(self.data.storageIP())) {
                self.data.storageIP(srData.ipaddresses[0]);
            }
            var writeCacheSize = 0;
            $.each(srData.partitions.WRITE, function(index, info) {
                if (info['usable'] === true) {
                    writeCacheSize += info['available'];
                }
            });
            self.data.writeBufferGlobal(writeCacheSize / 1024 / 1024 / 1024);
            self.data.writeBufferGlobalMax(writeCacheSize);
            self.loadingMetadata(false);
        };
        self.preValidate = function () {
            var validationResult = {valid: true, reasons: [], fields: []};
            $.Deferred(function (mtptDeferred) {
                self.loadingPrevalidations(true);
                generic.xhrAbort(self.checkMtptHandle);
                self.checkMtptHandle = api.post('storagerouters/' + self.data.storageRouter().guid() + '/check_mtpt', {data: {name: self.data.name()}})
                    .then(self.shared.tasks.wait)
                    .done(function (data) {
                        if (data === true) {
                            validationResult.valid = false;
                            validationResult.reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.mtpt_in_use', {what: self.data.name()}));
                            validationResult.fields.push('name');
                        }
                        mtptDeferred.resolve();
                    })
                    .fail(mtptDeferred.reject)
                    .always(function () {
                        self.preValidateResult(validationResult);
                        self.loadingPrevalidations(false);
                    })

            }).promise();
        };
        self.loadBackends = function () {
            return $.Deferred(function (albaDeferred) {
                generic.xhrAbort(self.fetchAlbaVPoolHandle);
                var relay = '', remoteInfo = {},
                    getData = {
                        contents: 'available'
                    };
                if (!self.data.localHost()) {
                    relay = 'relay/';
                    remoteInfo.ip = self.data.host();
                    remoteInfo.port = self.data.port();
                    remoteInfo.client_id = self.data.clientID();
                    remoteInfo.client_secret = self.data.clientSecret();
                }
                $.extend(getData, remoteInfo);
                self.loadingBackends(true);
                self.invalidAlbaInfo(false);
                self.fetchAlbaVPoolHandle = api.get(relay + 'alba/backends', {queryparams: getData})
                    .done(function (data) {
                        var available_backends = [], calls = [];
                        $.each(data.data, function (index, item) {
                            if (item.available === true) {
                                // Not using data.usages.size for GLOBAL backends because of issue: https://github.com/openvstorage/framework/issues/1802
                                // data.usages.size as of 2.10.0 does work for GLOBAL backends because of list-osds i/o asd-multistatistics
                                getData.contents = 'local_summary,name,usages,presets';
                                calls.push(
                                    api.get(relay + 'alba/backends/' + item.guid + '/', {queryparams: getData})
                                        .then(function (data) {
                                            var available_backend = false;
                                            if (data.scaling === 'LOCAL') {
                                                var backendSize = data.usages.size;
                                                if ((backendSize !== undefined && backendSize > 0)) {
                                                    available_backends.push(data);
                                                    available_backend = true;
                                                }
                                            } else if (data.scaling === 'GLOBAL') {
                                                $.each(data.local_summary.devices, function(color, amount) {
                                                    if (color !== 'gray' && amount > 0) {
                                                        available_backends.push(data);
                                                        available_backend = true;
                                                        return false;
                                                    }
                                                })
                                            }
                                            if (available_backend === true) {
                                                self.albaPresetMap()[data.guid] = {};
                                                $.each(data.presets, function (_, preset) {
                                                    self.albaPresetMap()[data.guid][preset.name] = preset.is_available;
                                                });
                                            }
                                        })
                                );
                            }
                        });
                        $.when.apply($, calls)
                            .then(function () {
                                if (available_backends.length > 0) {
                                    available_backends.sort(function (backend1, backend2) {
                                        return backend1.name.toLowerCase() < backend2.name.toLowerCase() ? -1 : 1;
                                    });
                                    self.data.backends(available_backends);
                                    if (self.data.backend() === undefined) {
                                        self.data.backend(available_backends[0]);
                                        self.data.preset(self.data.enhancedPresets()[0]);
                                    }
                                } else {
                                    self.data.backends([]);
                                    self.data.backend(undefined);
                                    self.data.preset(undefined);
                                }
                                if (self.data.vPool() !== undefined) {
                                    var metadata = self.data.vPool().metadata();
                                    if (metadata.hasOwnProperty('backend')) {
                                        $.each(self.data.backends(), function (index, backend) {
                                            if (backend.guid === metadata.backend.backend_info.alba_backend_guid) {
                                                self.data.backend(backend);
                                                $.each(self.data.enhancedPresets(), function (_, preset) {
                                                    if (preset.name === metadata.backend.backend_info.preset) {
                                                        self.data.preset(preset);
                                                    }
                                                });
                                            }
                                        });
                                    }
                                }
                                self.loadingBackends(false);
                            })
                            .done(albaDeferred.resolve)
                            .fail(function () {
                                self.data.backends([]);
                                self.data.backend(undefined);
                                self.data.preset(undefined);
                                self.loadingBackends(false);
                                self.invalidAlbaInfo(true);
                                albaDeferred.reject();
                            });
                    })
                    .fail(function () {
                        self.data.backends([]);
                        self.data.backend(undefined);
                        self.data.preset(undefined);
                        self.loadingBackends(false);
                        self.invalidAlbaInfo(true);
                        albaDeferred.reject();
                    });
            }).promise();
        };

        // Durandal
        self.activate = function () {
            self.hostSubscription = self.data.host.subscribe(self.resetBackends);
            self.portSubscription = self.data.port.subscribe(self.resetBackends);
            self.clientIDSubscription = self.data.clientID.subscribe(self.resetBackends);
            self.clientSecretSubscription = self.data.clientSecret.subscribe(self.resetBackends);

            // Subscriptions
            self.localHostSubscription = self.data.localHost.subscribe(function(local) {
                self.data.host('');
                self.data.port(80);
                self.data.clientID('');
                self.data.clientSecret('');
                if (local === true && self.data.backends().length === 0) {
                    self.loadBackends();
                }
            });
            self.scoSizeSubscription = self.data.scoSize.subscribe(function(size) {
                if (size < 128) {
                    self.data.writeBufferVolume.min = 128;
                } else {
                    self.data.writeBufferVolume.min = 256;
                }
                self.data.writeBufferVolume(self.data.writeBufferVolume());
            });
            self.storageRouterSubscription = self.data.storageRouter.subscribe(function (storageRouter) {
                if (storageRouter === undefined) {
                    return;
                }
                self.loadingMetadata(true);
                var map = self.srMetadataMap(), srGuid = storageRouter.guid();
                if (!map.hasOwnProperty(srGuid)) {
                    map[srGuid] = undefined;
                    generic.xhrAbort(self.loadSRMetadataHandle);
                    self.loadSRMetadataHandle = api.post('storagerouters/' + srGuid + '/get_metadata')
                        .then(self.shared.tasks.wait)
                        .done(function (srData) {
                            map[storageRouter.guid()] = srData;
                            self.fillSRData(srData);
                            self.srMetadataMap(map);
                        });
                } else if (map[srGuid] !== undefined) {
                    self.fillSRData(map[srGuid]);
                }
            });

            var promise;
            if (self.data.vPool() !== undefined) {
                promise = self.data.vPool().loadStorageRouters();
            } else {
                promise = $.Deferred(function (deferred) {
                    deferred.resolve();
                }).promise();
            }
            promise.then(function () {
                generic.xhrAbort(self.loadStorageRoutersHandle);
                return self.loadStorageRoutersHandle = api.get('storagerouters', {queryparams: {contents: 'storagedrivers,features', sort: 'name'}})
                    .done(function (data) {
                        var guids = [], srdata = {};
                        $.each(data.data, function (index, item) {
                            guids.push(item.guid);
                            srdata[item.guid] = item;
                        });
                        generic.crossFiller(
                            guids, self.data.storageRoutersAvailable,
                            function (guid) {
                                if (self.data.vPool() === undefined || !self.data.vPool().storageRouterGuids().contains(guid)) {
                                    return new StorageRouter(guid);
                                }
                            }, 'guid'
                        );
                        generic.crossFiller(
                            guids, self.data.storageRoutersUsed,
                            function (guid) {
                                if (self.data.vPool() !== undefined && self.data.vPool().storageRouterGuids().contains(guid)) {
                                    return new StorageRouter(guid);
                                }
                            }, 'guid'
                        );
                        $.each(self.data.storageRoutersAvailable(), function (index, storageRouter) {
                            storageRouter.fillData(srdata[storageRouter.guid()]);
                        });
                        $.each(self.data.storageRoutersUsed(), function (index, storageRouter) {
                            storageRouter.fillData(srdata[storageRouter.guid()]);
                        });
                        self.data.storageRoutersAvailable.sort(function (sr1, sr2) {
                            return sr1.name() < sr2.name() ? -1 : 1;
                        });
                        self.data.storageRoutersUsed.sort(function (sr1, sr2) {
                            return sr1.name() < sr2.name() ? -1 : 1;
                        });
                        if (self.data.storageRouter() === undefined && self.data.storageRoutersAvailable().length > 0) {
                            self.data.storageRouter(self.data.storageRoutersAvailable()[0]);
                        }
                        if (self.data.vPool() !== undefined && self.data.vPool().metadata !== undefined) {
                            var metadata = self.data.vPool().metadata();
                            if (metadata.backend.hasOwnProperty('caching_info') && metadata.backend.caching_info.hasOwnProperty(self.data.storageRoutersUsed()[0].guid())) {
                                self.data.fragmentCacheOnRead(metadata.backend.caching_info[self.data.storageRoutersUsed()[0].guid()].fragment_cache_on_read);
                                self.data.fragmentCacheOnWrite(metadata.backend.caching_info[self.data.storageRoutersUsed()[0].guid()].fragment_cache_on_write);
                                self.data.blockCacheOnRead(metadata.backend.caching_info[self.data.storageRoutersUsed()[0].guid()].block_cache_on_read);
                                self.data.blockCacheOnWrite(metadata.backend.caching_info[self.data.storageRoutersUsed()[0].guid()].block_cache_on_write);
                            }
                        }
                    })
                    .then(self.loadBackends);
            });
            if (generic.xhrCompleted(self.loadVPoolsHandle)) {
                self.loadVPoolsHandle = api.get('vpools', {
                    queryparams: {
                        contents: ''
                    }
                })
                    .done(function (data) {
                        var guids = [], vpData = {};
                        $.each(data.data, function (index, item) {
                            guids.push(item.guid);
                            vpData[item.guid] = item;
                        });
                        generic.crossFiller(
                            guids, self.data.vPools,
                            function (guid) {
                                return new VPool(guid);
                            }, 'guid'
                        );
                        $.each(self.data.vPools(), function (index, vpool) {
                            if (guids.contains(vpool.guid())) {
                                vpool.fillData(vpData[vpool.guid()]);
                            }
                        });
                    });
            }
            if (self.data.vPool() !== undefined) {
                var currentConfig = self.data.vPool().configuration();
                self.data.name(self.data.vPool().name());
                self.data.scoSize(currentConfig.sco_size);
                self.data.mdsSafety(currentConfig.mds_config.mds_safety);
                self.data.dtlEnabled(currentConfig.dtl_enabled);
                self.data.clusterSize(currentConfig.cluster_size);
                self.data.dtlMode({name: currentConfig.dtl_mode});
                self.data.writeBufferVolume(currentConfig.write_buffer);
                self.data.dtlTransportMode({name: currentConfig.dtl_transport});
                var metadata = self.data.vPool().metadata();
                if (metadata.hasOwnProperty('backend')) {
                    if (metadata.backend.hasOwnProperty('connection_info')) {
                        self.data.localHost(metadata.backend.connection_info.local);
                        if (metadata.backend.connection_info.local) {
                            self.data.clientID('');
                            self.data.clientSecret('');
                            self.data.host('');
                            self.data.port(80);
                        } else {
                            self.data.clientID(metadata.backend.connection_info.client_id);
                            self.data.clientSecret(metadata.backend.connection_info.client_secret);
                            self.data.host(metadata.backend.connection_info.host);
                            self.data.port(metadata.backend.connection_info.port);
                        }
                    }
                    if (metadata.backend.hasOwnProperty('caching_info')) {
                        var maxFCacheQuota = 0, maxBCacheQuota = 0;
                        $.each(metadata.backend.caching_info, function(srGuid, cachingInfo) {
                            if (cachingInfo.hasOwnProperty('quota_fc') && cachingInfo['quota_fc'] > maxFCacheQuota) {
                                maxFCacheQuota = cachingInfo['quota_fc'];
                            }
                            if (cachingInfo.hasOwnProperty('quota_bc') && cachingInfo['quota_bc'] > maxBCacheQuota) {
                                maxBCacheQuota = cachingInfo['quota_bc'];
                            }
                        });
                        if (maxFCacheQuota !== 0) {
                            self.data.cacheQuotaFC(maxFCacheQuota / Math.pow(1024.0, 3));
                            self.data.cacheQuotaFCConfigured(true);
                        }
                        if (maxBCacheQuota !== 0) {
                            self.data.cacheQuotaBC(maxBCacheQuota / Math.pow(1024.0, 3));
                            self.data.cacheQuotaBCConfigured(true);
                        }
                    }
                }
            }
        };
        self.deactivate = function() {
            self.hostSubscription.dispose();
            self.portSubscription.dispose();
            self.scoSizeSubscription.dispose();
            self.clientIDSubscription.dispose();
            self.localHostSubscription.dispose();
            self.clientSecretSubscription.dispose();
            self.storageRouterSubscription.dispose();
        }
    };
});
