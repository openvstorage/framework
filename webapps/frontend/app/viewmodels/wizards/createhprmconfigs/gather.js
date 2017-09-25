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
        self.loadProxyConfigHandle = undefined;

        // Observables
        self.loadingProxyConfig       = ko.observable(false);
        self.loadingProxyConfigFailed = ko.observable(false);
        self.proxyConfigLoaded        = ko.observable(false);

        // Computed
        self.canContinue = ko.computed(function() {
            var reasons = [], fields = [];
            if (!self.data.identifier.valid()) {
                fields.push('identifier');
                reasons.push($.t('ovs:wizards.create_hprm_configs.gather.invalid_identifier'));
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
                            self.data.useFC(data.fragment_cache[0] === 'alba');
                            if (data.fragment_cache[0] !== 'none') {
                                self.data.fragmentCacheOnRead(data.fragment_cache[1].cache_on_read);
                                self.data.fragmentCacheOnWrite(data.fragment_cache[1].cache_on_write);
                            }
                            if (self.data.useFC() === true) {
                                $.each(self.data.vPool().metadata(), function(key, value) {
                                    if (key === 'backend_aa_' + self.data.storageRouter().guid()) {
                                        self.data.localHostFC(value.connection_info.local);
                                    }
                                })
                            }
                            if (data.block_cache !== undefined) {
                                self.data.useBC(data.block_cache[0] === 'alba');
                                if (data.block_cache[0] !== 'none') {
                                    self.data.blockCacheOnRead(data.block_cache[1].cache_on_read);
                                    self.data.blockCacheOnWrite(data.block_cache[1].cache_on_write);
                                }
                                if (self.data.useBC() === true) {
                                    $.each(self.data.vPool().metadata(), function(key, value) {
                                        if (key === 'backend_bc_' + self.data.storageRouter().guid()) {
                                            self.data.localHostBC(value.connection_info.local);
                                        }
                                    })
                                }
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

        // Durandal
        self.activate = function() {
            if (self.loadingProxyConfig() === false) {  // To avoid Durandal running activate twice
                self.loadProxyConfig();
            }
        };
    };
});
