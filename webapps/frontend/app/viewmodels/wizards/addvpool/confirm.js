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
    './data',
    'ovs/api', 'ovs/generic', 'ovs/shared'
], function($, ko, data, api, generic, shared) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data   = data;
        self.shared = shared;

        // Computed
        self.canContinue = ko.computed(function() {
            return { value: true, reasons: [], fields: [] };
        });

        // Functions
        self.finish = function() {
            return $.Deferred(function(deferred) {
                var postData = {
                    call_parameters: {
                        vpool_name: self.data.name(),
                        connection_info: {
                            host: self.data.host(),
                            port: self.data.port(),
                            local: self.data.localHost(),
                            client_id: self.data.clientID(),
                            client_secret: self.data.clientSecret()
                        },
                        backend_info: {
                            preset: (self.data.preset() !== undefined ? self.data.preset().name : undefined),
                            alba_backend_guid: (self.data.backend() !== undefined ? self.data.backend().guid : undefined)
                        },
                        storage_ip: self.data.storageIP(),
                        storagerouter_ip: self.data.storageRouter().ipAddress(),
                        writecache_size: self.data.writeBufferGlobal(),
                        fragment_cache_on_read: self.data.fragmentCacheOnRead(),
                        fragment_cache_on_write: self.data.fragmentCacheOnWrite(),
                        block_cache_on_read: self.data.blockCacheOnRead(),
                        block_cache_on_write: self.data.blockCacheOnWrite(),
                        config_params: {
                            dtl_mode: (self.data.dtlEnabled() === true ? self.data.dtlMode().name : 'no_sync'),
                            sco_size: self.data.scoSize(),
                            cluster_size: self.data.clusterSize(),
                            write_buffer: self.data.writeBufferVolume(),
                            dtl_transport: self.data.dtlTransportMode().name
                        },
                        parallelism: {
                            proxies: self.data.proxyAmount()
                        }
                    }
                };

                if (self.data.useFC() === true) {
                    postData.call_parameters.connection_info_fc = {
                        host: self.data.hostFC(),
                        port: self.data.portFC(),
                        local: self.data.localHostFC(),
                        client_id: self.data.clientIDFC(),
                        client_secret: self.data.clientSecretFC()
                    };
                    postData.call_parameters.backend_info_fc = {
                        preset: (self.data.presetFC() !== undefined ? self.data.presetFC().name : undefined),
                        alba_backend_guid: (self.data.backendFC() !== undefined ? self.data.backendFC().guid : undefined)
                    };
                }
                if (self.data.useBC() === true) {
                    postData.call_parameters.connection_info_bc = {
                        host: self.data.hostBC(),
                        port: self.data.portBC(),
                        local: self.data.localHostBC(),
                        client_id: self.data.clientIDBC(),
                        client_secret: self.data.clientSecretBC()
                    };
                    postData.call_parameters.backend_info_bc = {
                        preset: (self.data.presetBC() !== undefined ? self.data.presetBC().name : undefined),
                        alba_backend_guid: (self.data.backendBC() !== undefined ? self.data.backendBC().guid : undefined)
                    };
                }
                if (data.vPool() === undefined) {
                    generic.alertInfo($.t('ovs:wizards.add_vpool.confirm.started'), $.t('ovs:wizards.add_vpool.confirm.in_progress', { what: self.data.name() }));
                } else {
                    generic.alertInfo($.t('ovs:wizards.extend_vpool.confirm.started'), $.t('ovs:wizards.extend_vpool.confirm.in_progress', { what: self.data.name() }));
                }
                api.post('storagerouters/' + self.data.storageRouter().guid() + '/add_vpool', { data: postData })
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        if (data.vPool() === undefined) {
                            generic.alertSuccess($.t('ovs:generic.saved'), $.t('ovs:wizards.add_vpool.confirm.success', { what: self.data.name() }));
                        } else {
                            generic.alertSuccess($.t('ovs:generic.saved'), $.t('ovs:wizards.extend_vpool.confirm.success', { what: self.data.name() }));
                        }
                        if (self.data.completed !== undefined) {
                            self.data.completed.resolve(true);
                        }
                    })
                    .fail(function() {
                        if (data.vPool() === undefined) {
                            generic.alertError($.t('ovs:generic.error'), $.t('ovs:generic.messages.errorwhile', { what: $.t('ovs:wizards.add_vpool.confirm.creating') }));
                        } else {
                            generic.alertError($.t('ovs:generic.error'), $.t('ovs:generic.messages.errorwhile', { what: $.t('ovs:wizards.extend_vpool.confirm.extending') }));
                        }
                        if (self.data.completed !== undefined) {
                            self.data.completed.resolve(false);
                        }
                    });
                deferred.resolve();
            }).promise();
        };
    };
});
