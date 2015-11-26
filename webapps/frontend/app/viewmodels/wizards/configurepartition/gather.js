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
    'plugins/router',
    'ovs/api', 'ovs/shared', 'ovs/generic',
    './data',
    '../../containers/backend', '../../containers/backendtype'
], function($, ko, router, api, shared, generic, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data                   = data;
        self.shared                 = shared;

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, reasons = [], fields = [];
            return { value: valid, reasons: reasons, fields: fields };
        });

        // Functions
        self.isEmpty = generic.isEmpty;
        self.finish = function() {
            return $.Deferred(function(deferred) {
                var roles = [], post_data = {
                    disk_guid: self.data.disk().guid(),
                    partition_guid: self.data.partition().guid(),
                    offset: self.data.partition().offset.raw(),
                    size: self.data.partition().size.raw()
                };
                $.each(self.data.roles(), function(index, roleInfo) {
                    roles.push(roleInfo.name.toUpperCase());
                });
                post_data.roles = roles;
                api.post('storagerouters/' + self.data.storageRouter().guid() + '/configure_disk', { data: post_data })
                        .then(shared.tasks.wait)
                        .done(function() {
                            generic.alertSuccess($.t('ovs:generic.saved'), $.t('ovs:wizards.configurepartition.confirm.success'));
                        })
                        .fail(function() {
                            generic.alertError($.t('ovs:generic.error'), $.t('ovs:generic.messages.errorwhile', { what: $.t('ovs:wizards.configurepartition.confirm.creating') }));
                        });
                generic.alertInfo($.t('ovs:wizards.configurepartition.confirm.started'), $.t('ovs:wizards.configurepartition.confirm.inprogress'));
                deferred.resolve();
            }).promise();
        };

        // Durandal
        self.activate = function() {
            api.post('storagerouters/' + self.data.storageRouter().guid() + '/get_metadata')
                .then(self.shared.tasks.wait)
                .done(function(metadata) {
                    self.data.currentUsage(metadata.partitions);
                    self.data.roles([]);
                    $.each(metadata.partitions, function(role, partitions) {
                        $.each(partitions, function(index, partition) {
                            if (partition.guid === self.data.partition().guid()) {
                                self.data.roles.push({name: role.toLowerCase()});
                            }
                        })
                    });
                });
        };
    };
});
