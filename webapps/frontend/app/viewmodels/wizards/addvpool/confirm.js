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
                var post_data = {
                    call_parameters: {
                        vpool_name: self.data.name(),
                        type: self.data.backend(),
                        backend_connection_info: {
                            host: self.data.host(),
                            port: self.data.port(),
                            username: self.data.accesskey(),
                            password: self.data.secretkey(),
                            backend: {
                                'backend': (self.data.backend() === 'alba' && self.data.albaBackend() !== undefined ? self.data.albaBackend().guid : undefined),
                                'metadata': (self.data.backend() === 'alba' && self.data.albaPreset() !== undefined ? self.data.albaPreset().name : undefined)
                            }
                        },
                        storage_ip: self.data.storageIP(),
                        storagerouter_ip: self.data.storageRouter().ipAddress(),
                        integratemgmt: self.data.integratemgmt(),
                        readcache_size: self.data.readCacheSize(),
                        writecache_size: self.data.writeCacheSize(),
                        fragment_cache_on_read: self.data.fragmentCacheOnRead(),
                        fragment_cache_on_write: self.data.fragmentCacheOnWrite(),
                        config_params: {
                            'dtl_mode': self.data.dtlMode().name,
                            'sco_size': self.data.scoSize(),
                            'dedupe_mode': self.data.dedupeMode(),
                            'cluster_size': self.data.clusterSize(),
                            'write_buffer': self.data.writeBuffer(),
                            'dtl_transport': self.data.dtlTransportMode().name,
                            'cache_strategy': self.data.cacheStrategy()
                        }
                    }
                };

                if (self.data.backend() === 'distributed') {
                    post_data.call_parameters.distributed_mountpoint = self.data.distributedMtpt();
                }
                if (self.data.useAA() === true) {
                    post_data.call_parameters.backend_connection_info_aa = {
                        host: self.data.aaHost(),
                        port: self.data.aaPort(),
                        username: self.data.aaAccesskey(),
                        password: self.data.aaSecretkey(),
                        backend: {
                            'backend': (self.data.backend() === 'alba' && self.data.albaAABackend() !== undefined ? self.data.albaAABackend().guid : undefined),
                            'metadata': (self.data.backend() === 'alba' && self.data.albaAAPreset() !== undefined ? self.data.albaAAPreset().name : undefined)
                        }
                    }
                }
                if (data.vPoolAdd() === true) {
                    generic.alertInfo($.t('ovs:wizards.add_vpool.confirm.started'), $.t('ovs:wizards.add_vpool.confirm.in_progress', { what: self.data.name() }));
                } else {
                    generic.alertInfo($.t('ovs:wizards.extend_vpool.confirm.started'), $.t('ovs:wizards.extend_vpool.confirm.in_progress', { what: self.data.name() }));
                }
                api.post('storagerouters/' + self.data.storageRouter().guid() + '/add_vpool', { data: post_data })
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        if (data.vPoolAdd() === true) {
                            generic.alertSuccess($.t('ovs:generic.saved'), $.t('ovs:wizards.add_vpool.confirm.success', { what: self.data.name() }));
                        } else {
                            generic.alertSuccess($.t('ovs:generic.saved'), $.t('ovs:wizards.extend_vpool.confirm.success', { what: self.data.name() }));
                        }
                        if (self.data.completed !== undefined) {
                            self.data.completed.resolve(true);
                        }
                    })
                    .fail(function() {
                        if (data.vPoolAdd() === true) {
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
