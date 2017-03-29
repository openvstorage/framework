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
        self.loadBackendsHandle    = undefined;
        self.loadProxyConfigHandle = undefined;

        // Observables
        self.albaPresetMap            = ko.observable({});
        self.albaBackends             = ko.observableArray([]);
        self.cacheBehaviors           = ko.observableArray(['write', 'read', 'rw', 'none']);
        self.loadingBackends          = ko.observable(false);
        self.loadingBackendsFailed    = ko.observable(false);
        self.loadingProxyConfig       = ko.observable(false);
        self.loadingProxyConfigFailed = ko.observable(false);
        self.localBackendsChecked     = ko.observable(false);
        self.localBackendsAvailable   = ko.observable(false);
        self.proxyConfigLoaded        = ko.observable(false);
        self.reloadingBackends        = ko.observable(false);

        // Computed
        self.cacheBehavior = ko.computed({
            read: function() {
                if (self.data.cacheOnRead() && self.data.cacheOnWrite()) {
                    return 'rw';
                }
                if (self.data.cacheOnRead() || self.data.cacheOnWrite()) {
                    return self.data.cacheOnRead() ? 'read' : 'write';
                }
                return 'none';
            },
            write: function(cache) {
                self.data.cacheOnRead(['rw', 'read'].contains(cache));
                self.data.cacheOnWrite(['rw', 'write'].contains(cache));
                if (cache === 'none') {
                    self.data.cacheUseAlba(false);
                }
            }
        });
        self.isPresetAvailable = ko.computed(function() {
            if (self.data.albaBackend() !== undefined && self.data.albaPreset() !== undefined && self.data.cacheUseAlba() === true) {
                var guid = self.data.albaBackend().guid,
                    name = self.data.albaPreset().name;
                if (self.albaPresetMap().hasOwnProperty(guid) && self.albaPresetMap()[guid].hasOwnProperty(name)) {
                    return self.albaPresetMap()[guid][name];
                }
            }
            return true;
        });
        self.canContinue = ko.computed(function() {
            var reasons = [], fields = [];
            if (self.loadingProxyConfig() === true) {
                reasons.push($.t('ovs:wizards.create_hprm_configs.gather.loading_config_information'));
            }
            if (self.data.cacheOnRead() || self.data.cacheOnWrite()) {
                if (self.data.cacheUseAlba() === false ) {
                    var path = self.data.localPath();
                    if (path === '' || path.endsWith('/.') || path.includes('..') || path.includes('/./')) {
                        fields.push('local_path');
                        reasons.push($.t('ovs:wizards.create_hprm_configs.gather.invalid_local_path'));
                    }
                } else {
                    if (self.data.albaBackend() === undefined && self.loadingBackendsFailed() === false) {
                        reasons.push($.t('ovs:wizards.create_hprm_configs.gather.choose_backend'));
                        fields.push('backend');
                    } else if (self.data.albaPreset() === undefined && self.loadingBackendsFailed() === false) {
                        reasons.push($.t('ovs:wizards.create_hprm_configs.gather.choose_preset'));
                        fields.push('preset');
                    }
                    if (!self.data.albaUseLocalBackend()) {
                        if (!self.data.albaHost.valid()) {
                            fields.push('host');
                            reasons.push($.t('ovs:wizards.create_hprm_configs.gather.invalid_host'));
                        }
                        if (self.data.albaClientID() === '' || self.data.albaClientSecret() === '') {
                            fields.push('clientid');
                            fields.push('clientsecret');
                            reasons.push($.t('ovs:wizards.create_hprm_configs.gather.no_credentials'));
                        }
                        if (self.loadingBackendsFailed()) {
                            reasons.push($.t('ovs:wizards.create_hprm_configs.gather.invalid_alba_info'));
                            fields.push('clientid');
                            fields.push('clientsecret');
                            fields.push('host');
                        }
                    }
                }
            }
            return { value: reasons.length === 0, reasons: reasons, fields: fields };
        });

        // Functions
        self.loadProxyConfig = function() {
            return $.Deferred(function(deferred) {
                if (!self.proxyConfigLoaded()) {
                    generic.xhrAbort(self.loadProxyConfigHandle);
                    self.loadingProxyConfig(true);
                    self.loadProxyConfigHandle = api.get('storagerouters/' + self.data.storageRouter().guid() + '/get_proxy_config', {queryparams: {vpool_guid: self.data.vPool().guid()}})
                        .then(self.shared.tasks.wait)
                        .done(function(data) {
                            self.data.hprmPort(data.port);
                            self.data.cacheUseAlba(data.fragment_cache[0] === 'alba');
                            if (data.fragment_cache[0] !== 'none') {
                                self.data.cacheOnRead(data.fragment_cache[1].cache_on_read);
                                self.data.cacheOnWrite(data.fragment_cache[1].cache_on_write);
                            }
                            if (self.data.cacheUseAlba() === true) {
                                $.each(self.data.vPool().metadata(), function(key, value) {
                                    if (key === 'backend_aa_' + self.data.storageRouter().guid()) {
                                        self.data.albaUseLocalBackend(value.connection_info.local);
                                    }
                                })
                            }
                        })
                        .fail(function() {
                            self.loadingProxyConfigFailed(true);
                        })
                        .always(function() {
                            self.loadingProxyConfig(false);
                            self.proxyConfigLoaded(true);
                            deferred.resolve();
                        });
                }
            }).promise();
        };
        self.loadBackends = function() {
            return $.Deferred(function(albaDeferred) {
                generic.xhrAbort(self.loadBackendsHandle);
                self.loadingBackends(true);
                var relay = '',
                    getData = {contents: 'asd_statistics,available,name,presets'},
                    remoteInfo = {},
                    albaBackendGuid = self.data.vPool().metadata().backend.backend_info.alba_backend_guid;
                if (!self.data.albaUseLocalBackend()) {
                    relay = 'relay/';
                    remoteInfo.ip = self.data.albaHost();
                    remoteInfo.port = self.data.albaPort();
                    remoteInfo.client_id = self.data.albaClientID().replace(/\s+/, "");
                    remoteInfo.client_secret = self.data.albaClientSecret().replace(/\s+/, "");
                }
                $.extend(getData, remoteInfo);
                self.loadBackendsHandle = api.get(relay + 'alba/backends', { queryparams: getData })
                    .done(function(data) {
                        var available_backends = [], calls = [];
                        $.each(data.data, function (index, item) {
                            if (item.available === true && item.guid !== albaBackendGuid) {
                                calls.push(
                                    api.get(relay + 'alba/backends/' + item.guid + '/', { queryparams: getData })
                                        .then(function(data) {
                                            if (Object.keys(data.asd_statistics).length > 0 || data.scaling === 'GLOBAL') {
                                                available_backends.push(data);
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
                            .then(function() {
                                if (available_backends.length > 0) {
                                    if (self.localBackendsChecked() === false) {
                                        self.localBackendsChecked(true);
                                        self.localBackendsAvailable(true);
                                    }
                                    // Fill out ALBA Backend and ALBA preset
                                    if (self.data.vPool().metadata().hasOwnProperty('backend_aa_' + self.data.storageRouter().guid())) {
                                        var backendInfo = self.data.vPool().metadata()['backend_aa_' + self.data.storageRouter().guid()].backend_info;
                                        $.each(available_backends, function(_, alba_backend) {
                                            if (alba_backend.guid === backendInfo.alba_backend_guid) {
                                                self.data.albaBackend(alba_backend);
                                                $.each(self.data.albaPresetEnhanced(), function(_, preset) {
                                                    if (preset.name === backendInfo.preset) {
                                                        self.data.albaPreset(preset);
                                                        return false;
                                                    }
                                                });
                                                return false;
                                            }
                                        });
                                    }
                                    available_backends.sort(function(backend1, backend2) {
                                        return backend1.name.toLowerCase() < backend2.name.toLowerCase() ? -1 : 1;
                                    });
                                    if (self.data.albaBackend() === undefined) {
                                        self.data.albaBackend(available_backends[0]);
                                        self.data.albaPreset(self.data.albaPresetEnhanced()[0]);
                                    }
                                    self.albaBackends(available_backends);
                                } else {
                                    self.albaBackends([]);
                                    self.data.albaBackend(undefined);
                                    self.data.albaPreset(undefined);
                                }
                                self.loadingBackends(false);
                            })
                            .done(albaDeferred.resolve)
                            .fail(function() {
                                self.albaBackends([]);
                                self.data.albaBackend(undefined);
                                self.data.albaPreset(undefined);
                                self.loadingBackends(false);
                                self.loadingBackendsFailed(true);
                                albaDeferred.reject();
                            });
                        self.loadingBackendsFailed(false);
                    })
                    .fail(function() {
                        self.albaBackends([]);
                        self.data.albaBackend(undefined);
                        self.data.albaPreset(undefined);
                        self.loadingBackends(false);
                        self.loadingBackendsFailed(true);
                        albaDeferred.reject();
                    });
            }).promise();
        };
        self.reloadBackends = function() {
            self.reloadingBackends(true);
            self.loadBackends()
                .always(function() {
                    self.reloadingBackends(false);
                });
        };
        self.resetBackendsAA = function() {
            self.albaBackends([]);
            self.data.albaBackend(undefined);
            self.data.albaPreset(undefined);
        };

        // Durandal
        self.activate = function() {
            if (self.loadingProxyConfig() === false) {  // To avoid Durandal running activate twice
                self.loadProxyConfig()
                    .then(function() {
                        if (self.loadingBackends() === false && self.albaBackends().length === 0) {
                            self.loadBackends();  // To avoid Durandal running activate twice
                        }
                    })
            }

            // Subscriptions
            self.albaHostSubscription = self.data.albaHost.subscribe(self.resetBackendsAA);
            self.albaPortSubscription = self.data.albaPort.subscribe(self.resetBackendsAA);
            self.albaClientIDSubscription = self.data.albaClientID.subscribe(self.resetBackendsAA);
            self.albaClientSecretSubscription = self.data.albaClientSecret.subscribe(self.resetBackendsAA);
            self.albaUseLocalBackendSubscription = self.data.albaUseLocalBackend.subscribe(function(local) {
                self.albaBackends([]);
                self.data.albaBackend(undefined);
                if (self.data.vPool().metadata().hasOwnProperty('backend_aa_' + self.data.storageRouter().guid())) {
                    var connectionInfo = self.data.vPool().metadata()['backend_aa_' + self.data.storageRouter().guid()].connection_info;
                    self.data.albaHost(connectionInfo.host);
                    self.data.albaPort(connectionInfo.port);
                    self.data.albaClientID(connectionInfo.client_id);
                    self.data.albaClientSecret(connectionInfo.client_secret);
                }
                if (local === true || (self.data.albaHost() !== '')) {
                    self.reloadBackends();
                }
            });
        };
        self.deactivate = function() {
            self.albaHostSubscription.dispose();
            self.albaPortSubscription.dispose();
            self.albaClientIDSubscription.dispose();
            self.albaClientSecretSubscription.dispose();
            self.albaUseLocalBackendSubscription.dispose();
        }
    };
});
