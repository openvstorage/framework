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
], function($, ko, shared, api, generic, StorageRouter, StorageDriver, VPool, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data   = data;
        self.shared = shared;

        // Handles
        self.checkS3Handle            = undefined;
        self.checkMtptHandle          = undefined;
        self.fetchAlbaVPoolHandle     = undefined;
        self.loadStorageRoutersHandle = undefined;

        // Observables
        self.albaBackendLoading = ko.observable(false);
        self.albaPresetMap      = ko.observable({});
        self.invalidAlbaInfo    = ko.observable(false);
        self.preValidateResult  = ko.observable({ valid: true, reasons: [], fields: [] });

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, showErrors = false, reasons = [], fields = [], preValidation = self.preValidateResult();
            if (self.data.vPool() === undefined) {
                if (!self.data.name.valid()) {
                    valid = false;
                    fields.push('name');
                    reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.invalid_name'));
                }
                else {
                    $.each(self.data.vPools(), function (index, vpool) {
                        if (vpool.name() === self.data.name()) {
                            valid = false;
                            fields.push('name');
                            reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.duplicate_name'));
                        }
                    });
                }
                if (self.data.backend().match(/^.+_s3$/)) {
                    if (!self.data.host.valid()) {
                        valid = false;
                        fields.push('host');
                        reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.invalid_host'));
                    }
                    if (self.data.accesskey() === '' || self.data.secretkey() === '') {
                        valid = false;
                        fields.push('accesskey');
                        fields.push('secretkey');
                        reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.no_credentials'));
                    }
                }
                if (preValidation.valid === false) {
                    showErrors = true;
                    reasons = reasons.concat(preValidation.reasons);
                    fields = fields.concat(preValidation.fields);
                }
            }
            if (self.data.backend() === 'alba' && self.data.editBackend()) {
                if (self.data.albaBackend() === undefined) {
                    valid = false;
                    reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.choose_backend'));
                    fields.push('backend');
                }
                if (self.invalidAlbaInfo() && !self.data.localHost()) {
                    valid = false;
                    reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.invalid_alba_info'));
                    fields.push('clientid');
                    fields.push('clientsecret');
                    fields.push('host');
                }
            }
            return { value: valid, showErrors: showErrors, reasons: reasons, fields: fields };
        });
        self.isPresetAvailable = ko.computed(function() {
            var presetAvailable = true;
            if (self.data.albaBackend() !== undefined && self.data.albaPreset() !== undefined) {
                var guid = self.data.albaBackend().guid,
                    name = self.data.albaPreset().name;
                if (self.albaPresetMap().hasOwnProperty(guid) && self.albaPresetMap()[guid].hasOwnProperty(name)) {
                    presetAvailable = self.albaPresetMap()[guid][name];
                }
            }
            return presetAvailable;
        });

        // Functions
        self.preValidate = function() {
            var validationResult = { valid: true, reasons: [], fields: [] };
            return $.Deferred(function(deferred) {
                $.when.apply($, [
                    $.Deferred(function(s3deferred) {
                        if (self.data.backend().match(/^.+_s3$/)) {
                            generic.xhrAbort(self.checkS3Handle);
                            var postData = {
                                host: self.data.host(),
                                port: self.data.port(),
                                accesskey: self.data.accesskey(),
                                secretkey: self.data.secretkey()
                            };
                            self.checkS3Handle = api.post('storagerouters/' + self.data.storageRouter().guid() + '/check_s3', { data: postData })
                                .then(self.shared.tasks.wait)
                                .done(function(data) {
                                    if (!data) {
                                        validationResult.valid = false;
                                        validationResult.reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.invalid_s3_info'));
                                        validationResult.fields.push('accesskey');
                                        validationResult.fields.push('secretkey');
                                        validationResult.fields.push('host');
                                    }
                                    s3deferred.resolve();
                                })
                                .fail(s3deferred.reject);
                        } else {
                            s3deferred.resolve();
                        }
                    }).promise(),
                    $.Deferred(function(mtptDeferred) {
                        generic.xhrAbort(self.checkMtptHandle);
                        var postData = {
                            name: self.data.name()
                        };
                        self.checkMtptHandle = api.post('storagerouters/' + self.data.storageRouter().guid() + '/check_mtpt', { data: postData })
                            .then(self.shared.tasks.wait)
                            .done(function(data) {
                                if (!data) {
                                    validationResult.valid = false;
                                    validationResult.reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.mtpt_in_use', { what: self.data.name() }));
                                    validationResult.fields.push('name');
                                }
                                mtptDeferred.resolve();
                            })
                            .fail(mtptDeferred.reject);
                    }).promise()
                ])
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
        self.next = function() {
            $.each(self.data.storageRoutersAvailable(), function(index, storageRouter) {
                if (storageRouter === self.data.storageRouter()) {
                    $.each(self.data.dtlTransportModes(), function (i, key) {
                        if (key.name === 'rdma') {
                            self.data.dtlTransportModes()[i].disabled = storageRouter.rdmaCapable() === undefined ? true : !storageRouter.rdmaCapable();
                            return false;
                        }
                    });
                }
            });
            if (self.data.vPool() !== undefined) {
                return $.Deferred(function(deferred) {
                    var calls = [];
                    generic.crossFiller(
                        self.data.vPool().storageDriverGuids(), self.data.storageDrivers,
                        function(guid) {
                            var storageDriver = new StorageDriver(guid);
                            calls.push(storageDriver.load());
                            return storageDriver;
                        }, 'guid'
                    );
                    $.when.apply($, calls)
                        .done(deferred.resolve)
                        .fail(deferred.reject);
                });
            }
        };
        self.loadAlbaBackends = function() {
            return $.Deferred(function(albaDeferred) {
                generic.xhrAbort(self.fetchAlbaVPoolHandle);
                var relay = '', remoteInfo = {},
                    getData = {
                        backend_type: 'alba',
                        contents: '_dynamics'
                    };
                if (!self.data.localHost()) {
                    relay = 'relay/';
                    remoteInfo.ip = self.data.host();
                    remoteInfo.port = self.data.port();
                    remoteInfo.client_id = self.data.accesskey();
                    remoteInfo.client_secret = self.data.secretkey();
                }
                $.extend(getData, remoteInfo);
                self.albaBackendLoading(true);
                self.invalidAlbaInfo(false);
                self.fetchAlbaVPoolHandle = api.get(relay + 'backends', { queryparams: getData })
                    .done(function(data) {
                        var available_backends = [], calls = [];
                        $.each(data.data, function (index, item) {
                            if (item.available === true) {
                                calls.push(
                                    api.get(relay + 'alba/backends/' + item.linked_guid + '/', { queryparams: getData })
                                        .then(function(data) {
                                            if (data.available === true) {
                                                var asdsFound = false;
                                                $.each(data.asd_statistics, function(key, value) {  // As soon as we enter loop, we know at least 1 ASD is linked to this backend
                                                    asdsFound = true;
                                                    return false;
                                                });
                                                if (asdsFound === true) {
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
                                    self.data.albaBackends(available_backends);
                                    self.data.albaBackend(available_backends[0]);
                                    self.data.albaPreset(available_backends[0].presets[0]);
                                } else {
                                    self.data.albaBackends([]);
                                    self.data.albaBackend(undefined);
                                    self.data.albaPreset(undefined);
                                }
                                self.albaBackendLoading(false);
                            })
                            .done(albaDeferred.resolve)
                            .fail(function() {
                                self.data.albaBackends([]);
                                self.data.albaBackend(undefined);
                                self.data.albaPreset(undefined);
                                self.albaBackendLoading(false);
                                self.invalidAlbaInfo(true);
                                albaDeferred.reject();
                            });
                    })
                    .fail(function() {
                        self.data.albaBackends([]);
                        self.data.albaBackend(undefined);
                        self.data.albaPreset(undefined);
                        self.albaBackendLoading(false);
                        self.invalidAlbaInfo(true);
                        albaDeferred.reject();
                    });
            }).promise();
        };

        // Durandal
        self.activate = function() {
            generic.xhrAbort(self.loadStorageRoutersHandle);
            self.loadStorageRoutersHandle = api.get('storagerouters', {
                queryparams: {
                    contents: 'storagedrivers',
                    sort: 'name'
                }
            })
                .done(function(data) {
                    var guids = [], srdata = {};
                    $.each(data.data, function(index, item) {
                        guids.push(item.guid);
                        srdata[item.guid] = item;
                    });
                    generic.crossFiller(
                        guids, self.data.storageRoutersAvailable,
                        function(guid) {
                            if (self.data.vPool() === undefined || !self.data.vPool().storageRouterGuids().contains(guid)) {
                                return new StorageRouter(guid);
                            }
                        }, 'guid'
                    );
                    generic.crossFiller(
                        guids, self.data.storageRoutersUsed,
                        function(guid) {
                            if (self.data.vPool() !== undefined && self.data.vPool().storageRouterGuids().contains(guid)) {
                                return new StorageRouter(guid);
                            }
                        }, 'guid'
                    );
                    $.each(self.data.storageRoutersAvailable(), function(index, storageRouter) {
                        storageRouter.fillData(srdata[storageRouter.guid()]);
                    });
                    $.each(self.data.storageRoutersUsed(), function(index, storageRouter) {
                        storageRouter.fillData(srdata[storageRouter.guid()]);
                    });
                    self.data.storageRoutersAvailable.sort(function(sr1, sr2) {
                        return sr1.name() < sr2.name() ? -1 : 1;
                    });
                    self.data.storageRoutersUsed.sort(function(sr1, sr2) {
                        return sr1.name() < sr2.name() ? -1 : 1;
                    });
                    if (self.data.storageRouter() === undefined && self.data.storageRoutersAvailable().length > 0) {
                        self.data.storageRouter(self.data.storageRoutersAvailable()[0]);
                    }
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
                self.data.backend(self.data.vPool().backendType().code());
                self.data.scoSize(currentConfig.sco_size);
                self.data.dedupeMode(currentConfig.dedupe_mode);
                self.data.dtlEnabled(currentConfig.dtl_enabled);
                self.data.clusterSize(currentConfig.cluster_size);
                self.data.dtlMode({name: currentConfig.dtl_mode});
                self.data.writeBuffer(currentConfig.write_buffer);
                self.data.cacheStrategy(currentConfig.cache_strategy);
                self.data.dtlTransportMode({name: currentConfig.dtl_transport});
                var metadata = self.data.vPool().metadata();
                if (self.data.vPool().backendType().code() === 'alba') {
                    if (metadata.hasOwnProperty('backend') && metadata.backend.hasOwnProperty('connection')) {
                        // Created in or after 2.7.0
                        self.data.v260Migration(false);
                        self.data.localHost(metadata.backend.connection.local);
                        self.data.fragmentCacheOnRead(metadata.backend.backend_info.fragment_cache_on_read);
                        self.data.fragmentCacheOnWrite(metadata.backend.backend_info.fragment_cache_on_write);
                        if (metadata.backend.connection.local) {
                            self.data.accesskey('');
                            self.data.secretkey('');
                            self.data.host('');
                            self.data.port(80);
                        } else {
                            self.data.accesskey(metadata.backend.connection.client_id);
                            self.data.secretkey(metadata.backend.connection.client_secret);
                            self.data.host(metadata.backend.connection.host);
                            self.data.port(metadata.backend.connection.port);
                        }
                        self.loadAlbaBackends()
                            .done(function () {
                                $.each(self.data.albaBackends(), function (_, albaBackend) {
                                    if (albaBackend.guid === metadata.backend.backend_guid) {
                                        self.data.albaBackend(albaBackend);
                                        $.each(albaBackend.presets, function (_, preset) {
                                            if (preset.name === metadata.backend.preset) {
                                                self.data.albaPreset(preset);
                                            }
                                        });
                                    }
                                });
                            });
                    } else {
                        // Created before 2.7.0
                        self.data.v260Migration(true);
                    }
                }
            }
        };
    };
});
