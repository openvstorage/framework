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
        self.data = data;

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
                        connection_backend: {'backend': (self.data.backend() === 'alba' && self.data.albaBackend() !== undefined ? self.data.albaBackend().guid : undefined),
                                             'metadata': (self.data.backend() === 'alba' && self.data.albaPreset() !== undefined ? self.data.albaPreset().name : undefined)},
                        storage_ip: self.data.storageIP(),
                        integratemgmt: self.data.integratemgmt(),
                        readcache_size: self.data.readCacheSize(),
                        writecache_size: self.data.writeCacheSize()
                    }
                };
                var configParams = undefined;
                if (self.data.extendVpool() === false) {
                    configParams = {
                        'dtl_mode': self.data.dtlMode(),
                        'sco_size': self.data.scoSize(),
                        'dedupe_mode': self.data.dedupeMode(),
                        'dtl_enabled': self.data.dtlEnabled(),
                        'dtl_location': self.data.dtlLocation(),
                        'write_buffer': self.data.writeBuffer(),
                        'dtl_transport': self.data.dtlTransportMode().name,
                        'cache_strategy': self.data.cacheStrategy()
                    }
                }
                if (self.data.backend() === 'distributed') {
                    post_data.call_parameters.distributed_mountpoint = self.data.distributedMtpt();
                }
                post_data.call_parameters.config_params = configParams;
                var target_guid;
                if (self.data.extendVpool() === true) {
                    target_guid = self.data.storageRouter().guid()
                } else {
                    target_guid = self.data.target().guid()
                }
                api.post('storagerouters/' + target_guid + '/add_vpool', { data: post_data })
                        .then(shared.tasks.wait)
                        .done(function() {
                            generic.alertSuccess($.t('ovs:generic.saved'), $.t('ovs:wizards.addvpool.confirm.success', { what: self.data.name() }));
                        })
                        .fail(function() {
                            generic.alertError($.t('ovs:generic.error'), $.t('ovs:generic.messages.errorwhile', { what: $.t('ovs:wizards.addvpool.confirm.creating') }));
                        })
                        .always(function() {
                            if (self.data.completed !== undefined) {
                                self.data.completed.resolve(true);
                            }
                        });
                generic.alertInfo($.t('ovs:wizards.addvpool.confirm.started'), $.t('ovs:wizards.addvpool.confirm.inprogress', { what: self.data.name() }));
                deferred.resolve();
            }).promise();
        };
    };
});
