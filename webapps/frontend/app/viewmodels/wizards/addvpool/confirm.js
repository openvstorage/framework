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
                            username: self.data.clientID(),
                            password: self.data.clientSecret()
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
                        config_params: {
                            dtl_mode: (self.data.dtlEnabled() === true ? self.data.dtlMode().name : 'no_sync'),
                            sco_size: self.data.scoSize(),
                            cluster_size: self.data.clusterSize(),
                            write_buffer: self.data.writeBufferVolume(),
                            dtl_transport: self.data.dtlTransportMode().name
                        }
                    }
                };

                if (self.data.useAA() === true) {
                    postData.call_parameters.connection_info_aa = {
                        host: self.data.hostAA(),
                        port: self.data.portAA(),
                        local: self.data.localHostAA(),
                        username: self.data.clientIDAA(),
                        password: self.data.clientSecretAA()
                    };
                    postData.call_parameters.backend_info_aa = {
                        preset: (self.data.presetAA() !== undefined ? self.data.presetAA().name : undefined),
                        alba_backend_guid: (self.data.backendAA() !== undefined ? self.data.backendAA().guid : undefined)
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
