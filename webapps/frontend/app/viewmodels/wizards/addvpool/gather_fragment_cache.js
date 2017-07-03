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
        self.backendsFC             = ko.observableArray([]);
        self.fragmentCacheSettings  = ko.observableArray(['write', 'read', 'rw', 'none']);
        self.invalidAlbaInfo        = ko.observable(false);
        self.loadingBackends        = ko.observable(false);
        self.localBackendsAvailable = ko.observable(true);
        self.reUsedStorageRouter    = ko.observable();  // Connection info for this storagerouter will be used for accelerated ALBA

        // Computed
        self.isPresetAvailable = ko.computed(function() {
            var presetAvailable = true;
            if (self.data.backendFC() !== undefined && self.data.presetFC() !== undefined && self.data.useFC() === true) {
                var guid = self.data.backendFC().guid,
                    name = self.data.presetFC().name;
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
                if (self.data.vPool().metadata().hasOwnProperty('backend_fc_' + sr.guid())) {
                    temp.push(sr);
                }
            });
            temp.unshift(undefined);  // Insert undefined as element 0
            return temp;
        });
        self.canContinue = ko.computed(function() {
            var showErrors = false, reasons = [], fields = [];
            if (self.data.useFC()) {
                if (self.loadingBackends() === true) {
                    reasons.push($.t('ovs:wizards.add_vpool.gather_fragment_cache.backends_loading'));
                } else {
                    if (self.data.backendFC() === undefined && self.invalidAlbaInfo() === false) {
                        reasons.push($.t('ovs:wizards.add_vpool.gather_fragment_cache.choose_backend'));
                        fields.push('backend');
                    } else if (self.data.presetFC() === undefined && self.invalidAlbaInfo() === false) {
                        reasons.push($.t('ovs:wizards.add_vpool.gather_fragment_cache.choose_preset'));
                        fields.push('preset');
                    }
                    if (!self.data.localHostFC()) {
                        if (!self.data.hostFC.valid()) {
                            fields.push('host');
                            reasons.push($.t('ovs:wizards.add_vpool.gather_fragment_cache.invalid_host'));
                        }
                        if (self.data.clientIDFC() === '' || self.data.clientSecretFC() === '') {
                            fields.push('clientid');
                            fields.push('clientsecret');
                            reasons.push($.t('ovs:wizards.add_vpool.gather_fragment_cache.no_credentials'));
                        }
                        if (self.invalidAlbaInfo()) {
                            reasons.push($.t('ovs:wizards.add_vpool.gather_fragment_cache.invalid_alba_info'));
                            fields.push('clientid');
                            fields.push('clientsecret');
                            fields.push('host');
                        }
                    }
                    var quota = self.data.cacheQuotaFC();
                    if (quota !== undefined && quota !== '') {
                        if (isNaN(parseFloat(quota))) {
                            fields.push('quota');
                            reasons.push($.t('ovs:wizards.add_vpool.gather_fragment_cache.invalid_quota_nan'));
                        }
                    }
                }
            } else if (self.options !== undefined && self.options.customlocal === true && (self.data.fragmentCacheOnRead() || self.data.fragmentCacheOnWrite())) {
                var path = self.data.localPathFC();
                if (path === '' || path.endsWith('/.') || path.includes('..') || path.includes('/./')) {
                    fields.push('local_path');
                    reasons.push($.t('ovs:wizards.add_vpool.gather_fragment_cache.invalid_local_path'));
                }
                var localSize = self.data.localSizeFC();
                if (localSize !== undefined && localSize !== '') {
                    if (isNaN(parseFloat(localSize))) {
                        fields.push('localsize');
                        reasons.push($.t('ovs:wizards.add_vpool.gather_fragment_cache.invalid_local_size'));
                    }
                }
            }
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
                    self.data.useFC(false);
                }
            }
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

        // Functions
        self.resetBackendsFC = function() {
            self.backendsFC([]);
            self.data.backendFC(undefined);
            self.data.presetFC(undefined);
        };
        self.shouldSkip = function() {
            return $.Deferred(function(deferred) {
                if (self.data.vPool() !== undefined && !self.data.fragmentCacheOnRead() && !self.data.fragmentCacheOnWrite()) {
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
                if (!self.data.localHostFC()) {
                    relay = 'relay/';
                    remoteInfo.ip = self.data.hostFC();
                    remoteInfo.port = self.data.portFC();
                    remoteInfo.client_id = self.data.clientIDFC().replace(/\s+/, "");
                    remoteInfo.client_secret = self.data.clientSecretFC().replace(/\s+/, "");
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
                                    self.backendsFC(available_backends);
                                    if (self.data.backendFC() === undefined) {
                                        self.data.backendFC(available_backends[0]);
                                        self.data.presetFC(self.data.enhancedPresetsFC()[0]);
                                    }
                                } else {
                                    self.backendsFC([]);
                                    self.data.backendFC(undefined);
                                    self.data.presetFC(undefined);
                                }
                                self.loadingBackends(false);
                            })
                            .done(albaDeferred.resolve)
                            .fail(function() {
                                self.backendsFC([]);
                                self.data.backendFC(undefined);
                                self.data.presetFC(undefined);
                                self.loadingBackends(false);
                                self.invalidAlbaInfo(true);
                                albaDeferred.reject();
                            });
                    })
                    .fail(function() {
                        self.backendsFC([]);
                        self.data.backendFC(undefined);
                        self.data.presetFC(undefined);
                        self.loadingBackends(false);
                        self.invalidAlbaInfo(true);
                        albaDeferred.reject();
                    });
            }).promise();
        };

        // Durandal
        self.activate = function() {
            // Subscriptions
            self.useFCSubscription = self.data.useFC.subscribe(function(accelerated) {
                if (accelerated === true && self.backendsFC().length === 0) {
                    self.loadBackends();
                }
            });
            self.reUsedStorageRouterSubscription = self.reUsedStorageRouter.subscribe(function(sr) {
                if (sr === undefined && !self.data.localHostFC() && self.data.storageRoutersUsed().length > 0) {
                    self.data.hostFC('');
                    self.data.portFC(80);
                    self.data.clientIDFC('');
                    self.data.clientSecretFC('');
                }
                if (sr !== undefined && self.data.vPool() !== undefined && self.data.vPool().metadata().hasOwnProperty('backend_fc_' + sr.guid())) {
                    var md = self.data.vPool().metadata()['backend_fc_' + sr.guid()];
                    if (md.hasOwnProperty('connection_info')) {
                        self.data.hostFC(md.connection_info.host);
                        self.data.portFC(md.connection_info.port);
                        self.data.clientIDFC(md.connection_info.client_id);
                        self.data.clientSecretFC(md.connection_info.client_secret);
                    }
                }
            });
            self.hostFCSubscription = self.data.hostFC.subscribe(self.resetBackendsFC);
            self.portFCSubscription = self.data.portFC.subscribe(self.resetBackendsFC);
            self.clientIDFCSubscription = self.data.clientIDFC.subscribe(self.resetBackendsFC);
            self.clientSecretFCSubscription = self.data.clientSecretFC.subscribe(self.resetBackendsFC);
            self.localHostFCSubscription = self.data.localHostFC.subscribe(function(local) {
                self.data.hostFC('');
                self.data.portFC(80);
                self.data.clientIDFC('');
                self.data.clientSecretFC('');
                self.reUsedStorageRouter(undefined);
                if (local === true && self.data.useFC() === true && self.backendsFC().length === 0) {
                    self.loadBackends();
                }
            });

            if (options === undefined || options.allowlocalbackend !== true) {
                var localBackendsRequiredAmount = self.data.localHost() === true ? 2 : 1;
                if (self.data.backends().length >= localBackendsRequiredAmount) {
                    self.data.localHostFC(true);
                    self.localBackendsAvailable(true);
                } else {
                    self.data.localHostFC(false);
                    self.localBackendsAvailable(false);
                }
            }

            if (self.data.backend() !== undefined && self.data.backendFC() !== undefined && self.data.backend().guid === self.data.backendFC().guid) {
                self.backendsFC([]);
                $.each(self.data.backends(), function (_, backend) {
                    if (backend !== self.data.backend() && !self.backendsFC().contains(backend)) {
                        self.backendsFC().push(backend);
                    }
                });
                if (self.backendsFC().length === 0) {
                    self.data.backendFC(undefined);
                    self.data.presetFC(undefined);
                } else {
                    self.data.backendFC(self.backendsFC()[0]);
                    self.data.presetFC(self.data.enhancedPresetsFC()[0]);
                }
            }
            self.loadBackends();
        };
        self.deactivate = function() {
            self.useFCSubscription.dispose();
            self.hostFCSubscription.dispose();
            self.portFCSubscription.dispose();
            self.clientIDFCSubscription.dispose();
            self.localHostFCSubscription.dispose();
            self.clientSecretFCSubscription.dispose();
            self.reUsedStorageRouterSubscription.dispose();
        }
    };
});
