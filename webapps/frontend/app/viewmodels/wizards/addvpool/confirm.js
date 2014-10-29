// Copyright 2014 CloudFounders NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define([
    'jquery', 'knockout',
    '../../containers/vmachine', './data',
    'ovs/api', 'ovs/generic', 'ovs/shared'
], function($, ko, VMachine, data, api, generic, shared) {
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
                        connection_timeout: self.data.timeout(),
                        connection_username: self.data.accesskey(),
                        connection_password: self.data.secretkey(),
                        mountpoint_temp: self.data.mtptTemp(),
                        mountpoint_bfs: self.data.mtptBFS(),
                        mountpoint_md: self.data.mtptMD(),
                        mountpoint_readcache1: self.data.mtptReadCache1(),
                        mountpoint_readcache2: self.data.mtptReadCache2(),
                        mountpoint_writecache: self.data.mtptWriteCache(),
                        mountpoint_foc: self.data.mtptFOC(),
                        storage_ip: self.data.storageIP(),
                        vrouter_port: self.data.vRouterPort()
                    }
                };
                api.post('storagerouters/' + self.data.target().guid() + '/add_vpool', { data: post_data })
                        .then(shared.tasks.wait)
                        .done(function() {
                            generic.alertSuccess($.t('ovs:generic.saved'), $.t('ovs:wizards.addvpool.confirm.success', { what: self.data.name() }));
                        })
                        .fail(function() {
                            generic.alertError($.t('ovs:generic.error'), $.t('ovs:generic.messages.errorwhile', { what: $.t('ovs:wizards.addvpool.confirm.creating') }));
                        });
                generic.alertInfo($.t('ovs:wizards.addvpool.confirm.started'), $.t('ovs:wizards.addvpool.confirm.inprogress', { what: self.data.name() }));
                deferred.resolve();
            }).promise();
        };
    };
});
