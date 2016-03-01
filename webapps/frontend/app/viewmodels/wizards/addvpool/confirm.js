// Copyright 2014 iNuron NV
//
// Licensed under the Open vStorage Modified Apache License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
                        connection_host: self.data.host(),
                        connection_port: self.data.port(),
                        connection_username: self.data.accesskey(),
                        connection_password: self.data.secretkey(),
                        connection_backend: {'backend': (self.data.backend() === 'alba' && self.data.albaBackend() !== undefined ? self.data.albaBackend().guid() : undefined),
                                             'metadata': (self.data.backend() === 'alba' && self.data.albaPreset() !== undefined ? self.data.albaPreset().name : undefined)},
                        storage_ip: self.data.storageIP(),
                        integratemgmt: self.data.integratemgmt(),
                        readcache_size: self.data.readCacheSize(),
                        writecache_size: self.data.writeCacheSize(),
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
                if (data.vPoolAdd() === true) {
                    generic.alertInfo($.t('ovs:wizards.add_vpool.confirm.started'), $.t('ovs:wizards.add_vpool.confirm.in_progress', { what: self.data.name() }));
                } else {
                    generic.alertInfo($.t('ovs:wizards.extend_vpool.confirm.started'), $.t('ovs:wizards.extend_vpool.confirm.in_progress', { what: self.data.name() }));
                }
                api.post('storagerouters/' + self.data.target().guid() + '/add_vpool', { data: post_data })
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
