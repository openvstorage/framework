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
    return function(options) {
        var self = this;

        // Variables
        self.data    = options !== undefined && options.data !== undefined ? options.data : data;
        self.shared  = shared;
        self.options = options;

        // Handles
        self.fetchAlbaVPoolHandle = undefined;

        // Observables
        self.albaPresetMap          = ko.observable({});
        self.backendsBC             = ko.observableArray([]);
        self.blockCacheSettings     = ko.observableArray(['write', 'read', 'rw', 'none']);
        self.invalidAlbaInfo        = ko.observable(false);
        self.loadingBackends        = ko.observable(false);
        self.localBackendsAvailable = ko.observable(true);
        self.reUsedStorageRouter    = ko.observable();  // Connection info for this storagerouter will be used for accelerated ALBA

        // Computed
        self.isPresetAvailable = ko.computed(function() {
            var presetAvailable = true;
            if (self.data.backendBC() !== undefined && self.data.presetBC() !== undefined && self.data.useBC() === true) {
                var guid = self.data.backendBC().guid,
                    name = self.data.presetBC().name;
                if (self.albaPresetMap().hasOwnProperty(guid) && self.albaPresetMap()[guid].hasOwnProperty(name)) {
                    presetAvailable = self.albaPresetMap()[guid][name];
                }
            }
            return presetAvailable;
        });
        self.reUseableStorageRouters = ko.computed(function() {
            var temp = [];
            if (self.data.vPool() === undefined) {
                return temp;
            }
            $.each(self.data.storageRoutersUsed(), function(index, sr) {
                if (self.data.vPool().metadata().hasOwnProperty('backend_bc_' + sr.guid())) {
                    temp.push(sr);
                }
            });
            temp.unshift(undefined);  // Insert undefined as element 0
            return temp;
        });
        self.hasBlockCache = ko.computed(function() {
            return self.data.storageRouter() !== undefined &&
                self.data.storageRouter().features() !== undefined &&
                self.data.storageRouter().features().alba.features.contains('block-cache');
        });
        self.hasCacheQuota = ko.computed(function() {
            return self.data.storageRouter() !== undefined &&
                self.data.storageRouter().features() !== undefined &&
                self.data.storageRouter().features().alba.features.contains('cache-quota');
        });
        self.hasEE = ko.computed(function() {
            return self.data.storageRouter() !== undefined &&
                self.data.storageRouter().features() !== undefined &&
                self.data.storageRouter().features().alba.edition === 'enterprise';
        });
        self.canConfigureBCRW = ko.computed(function() {
            return self.data.vPoolAdd() && self.hasBlockCache();
        });
        self.canContinue = ko.computed(function() {
            var showErrors = false, reasons = [], fields = [];
            if (self.data.useBC()) {
                if (self.loadingBackends() === true) {
                    reasons.push($.t('ovs:wizards.add_vpool.gather_block_cache.backends_loading'));
                } else {
                    if (self.data.backendBC() === undefined && self.invalidAlbaInfo() === false) {
                        reasons.push($.t('ovs:wizards.add_vpool.gather_block_cache.choose_backend'));
                        fields.push('backend');
                    } else if (self.data.presetBC() === undefined && self.invalidAlbaInfo() === false) {
                        reasons.push($.t('ovs:wizards.add_vpool.gather_block_cache.choose_preset'));
                        fields.push('preset');
                    }
                    if (!self.data.localHostBC()) {
                        if (!self.data.hostBC.valid()) {
                            fields.push('host');
                            reasons.push($.t('ovs:wizards.add_vpool.gather_block_cache.invalid_host'));
                        }
                        if (self.data.clientIDBC() === '' || self.data.clientSecretBC() === '') {
                            fields.push('clientid');
                            fields.push('clientsecret');
                            reasons.push($.t('ovs:wizards.add_vpool.gather_block_cache.no_credentials'));
                        }
                        if (self.invalidAlbaInfo()) {
                            reasons.push($.t('ovs:wizards.add_vpool.gather_block_cache.invalid_alba_info'));
                            fields.push('clientid');
                            fields.push('clientsecret');
                            fields.push('host');
                        }
                    }
                    var quota = self.data.cacheQuotaBC();
                    if (quota !== undefined && quota !== '') {
                        if (isNaN(parseFloat(quota))) {
                            fields.push('quota');
                            reasons.push($.t('ovs:wizards.add_vpool.gather_block_cache.invalid_quota_nan'));
                        }
                    }
                }
            } else if (self.options !== undefined && self.options.customlocal === true && (self.data.blockCacheOnRead() || self.data.blockCacheOnWrite())) {
                var path = self.data.localPathBC();
                if (path === '' || path.endsWith('/.') || path.includes('..') || path.includes('/./')) {
                    fields.push('local_path');
                    reasons.push($.t('ovs:wizards.add_vpool.gather_block_cache.invalid_local_path'));
                }
                var localSize = self.data.localSizeBC();
                if (localSize !== undefined && localSize !== '') {
                    if (isNaN(parseFloat(localSize))) {
                        fields.push('localsize');
                        reasons.push($.t('ovs:wizards.add_vpool.gather_block_cache.invalid_local_size'));
                    }
                }
            }
            return { value: reasons.length === 0, showErrors: showErrors, reasons: reasons, fields: fields };
        });
        self.blockCacheSetting = ko.computed({
            read: function() {
                if (self.data.blockCacheOnRead() && self.data.blockCacheOnWrite()) {
                    return 'rw';
                }
                if (self.data.blockCacheOnRead() || self.data.blockCacheOnWrite()) {
                    return self.data.blockCacheOnRead() ? 'read' : 'write';
                }
                return 'none';
            },
            write: function(cache) {
                self.data.blockCacheOnRead(['rw', 'read'].contains(cache));
                self.data.blockCacheOnWrite(['rw', 'write'].contains(cache));
                if (cache === 'none') {
                    self.data.useBC(false);
                }
            }
        });

        // Functions
        self.resetBackendsBC = function() {
            self.backendsBC([]);
            self.data.backendBC(undefined);
            self.data.presetBC(undefined);
        };
        self.shouldSkip = function() {
            return $.Deferred(function(deferred) {
                if (self.data.vPool() !== undefined && !self.data.blockCacheOnRead() && !self.data.blockCacheOnWrite()) {
                    self.data.supportsBC(false);
                    deferred.resolve(true);
                } else {
                    deferred.resolve(false);
                }
            }).promise();
        };
        self.loadBackends = function() {
            return $.Deferred(function(albaDeferred) {
                generic.xhrAbort(self.fetchAlbaVPoolHandle);
                var relay = '', remoteInfo = {},
                    getData = {
                        contents: 'available'
                    };
                if (!self.data.localHostBC()) {
                    relay = 'relay/';
                    remoteInfo.ip = self.data.hostBC();
                    remoteInfo.port = self.data.portBC();
                    remoteInfo.client_id = self.data.clientIDBC().replace(/\s+/, "");
                    remoteInfo.client_secret = self.data.clientSecretBC().replace(/\s+/, "");
                }
                $.extend(getData, remoteInfo);
                self.loadingBackends(true);
                self.invalidAlbaInfo(false);
                self.fetchAlbaVPoolHandle = api.get(relay + 'alba/backends', { queryparams: getData })
                    .done(function(data) {
                        var available_backends = [], calls = [];
                        $.each(data.data, function (index, item) {
                            if (item.available === true) {
                                getData.contents = 'name,ns_statistics,presets,usages';
                                if (item.scaling === 'LOCAL') {
                                    getData.contents += ',osd_statistics';
                                }
                                calls.push(
                                    api.get(relay + 'alba/backends/' + item.guid + '/', { queryparams: getData })
                                        .then(function(data) {
                                            if (self.data.backend() === undefined || data.guid !== self.data.backend().guid) {
                                                var osd_statistics = data.osd_statistics;
                                                if ((osd_statistics !== undefined && Object.keys(osd_statistics).length > 0) || data.scaling === 'GLOBAL') {
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
                                    self.backendsBC(available_backends);
                                    if (self.data.backendBC() === undefined) {
                                        self.data.backendBC(available_backends[0]);
                                        self.data.presetBC(self.data.enhancedPresetsBC()[0]);
                                    }
                                } else {
                                    self.backendsBC([]);
                                    self.data.backendBC(undefined);
                                    self.data.presetBC(undefined);
                                }
                                self.loadingBackends(false);
                            })
                            .done(albaDeferred.resolve)
                            .fail(function() {
                                self.backendsBC([]);
                                self.data.backendBC(undefined);
                                self.data.presetBC(undefined);
                                self.loadingBackends(false);
                                self.invalidAlbaInfo(true);
                                albaDeferred.reject();
                            });
                    })
                    .fail(function() {
                        self.backendsBC([]);
                        self.data.backendBC(undefined);
                        self.data.presetBC(undefined);
                        self.loadingBackends(false);
                        self.invalidAlbaInfo(true);
                        albaDeferred.reject();
                    });
            }).promise();
        };

        // Durandal
        self.activate = function() {
            // Subscriptions
            self.useBCSubscription = self.data.useBC.subscribe(function(accelerated) {
                if (accelerated === true && self.backendsBC().length === 0) {
                    self.loadBackends();
                }
            });
            self.reUsedStorageRouterSubscription = self.reUsedStorageRouter.subscribe(function(sr) {
                if (sr === undefined && !self.data.localHostBC() && self.data.storageRoutersUsed().length > 0) {
                    self.data.hostBC('');
                    self.data.portBC(80);
                    self.data.clientIDBC('');
                    self.data.clientSecretBC('');
                }
                if (sr !== undefined && self.data.vPool() !== undefined && self.data.vPool().metadata().hasOwnProperty('backend_bc_' + sr.guid())) {
                    var md = self.data.vPool().metadata()['backend_bc_' + sr.guid()];
                    if (md.hasOwnProperty('connection_info')) {
                        self.data.hostBC(md.connection_info.host);
                        self.data.portBC(md.connection_info.port);
                        self.data.clientIDBC(md.connection_info.client_id);
                        self.data.clientSecretBC(md.connection_info.client_secret);
                    }
                }
            });
            self.hostBCSubscription = self.data.hostBC.subscribe(self.resetBackendsBC);
            self.portBCSubscription = self.data.portBC.subscribe(self.resetBackendsBC);
            self.clientIDBCSubscription = self.data.clientIDBC.subscribe(self.resetBackendsBC);
            self.clientSecretBCSubscription = self.data.clientSecretBC.subscribe(self.resetBackendsBC);
            self.localHostBCSubscription = self.data.localHostBC.subscribe(function(local) {
                self.data.hostBC('');
                self.data.portBC(80);
                self.data.clientIDBC('');
                self.data.clientSecretBC('');
                self.reUsedStorageRouter(undefined);
                if (local === true && self.data.useBC() === true && self.backendsBC().length === 0) {
                    self.loadBackends();
                }
            });

            if (options === undefined || options.allowlocalbackend !== true) {
                var localBackendsRequiredAmount = self.data.localHost() === true ? 2 : 1;
                if (self.data.backends().length >= localBackendsRequiredAmount) {
                    self.data.localHostBC(true);
                    self.localBackendsAvailable(true);
                } else {
                    self.data.localHostBC(false);
                    self.localBackendsAvailable(false);
                }
            }

            if (self.data.backend() !== undefined && self.data.backendBC() !== undefined && self.data.backend().guid === self.data.backendBC().guid) {
                self.backendsBC([]);
                $.each(self.data.backends(), function (_, backend) {
                    if (backend !== self.data.backend() && !self.backendsBC().contains(backend)) {
                        self.backendsBC().push(backend);
                    }
                });
                if (self.backendsBC().length === 0) {
                    self.data.backendBC(undefined);
                    self.data.presetBC(undefined);
                } else {
                    self.data.backendBC(self.backendsBC()[0]);
                    self.data.presetBC(self.data.enhancedPresetsBC()[0]);
                }
            }
            self.loadBackends();
            if (!self.hasBlockCache()) {
                self.data.blockCacheOnRead(false);
                self.data.blockCacheOnWrite(false);
                self.data.supportsBC(false);
            } else {
                self.data.supportsBC(true);
            }
        };
        self.deactivate = function() {
            self.useBCSubscription.dispose();
            self.hostBCSubscription.dispose();
            self.portBCSubscription.dispose();
            self.clientIDBCSubscription.dispose();
            self.localHostBCSubscription.dispose();
            self.clientSecretBCSubscription.dispose();
            self.reUsedStorageRouterSubscription.dispose();
        }
    };
});
