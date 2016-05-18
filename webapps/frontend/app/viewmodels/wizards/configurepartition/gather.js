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
    'ovs/api', 'ovs/shared', 'ovs/generic',
    './data',
    '../../containers/backend', '../../containers/backendtype'
], function($, ko, api, shared, generic, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data   = data;
        self.shared = shared;

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
                        .then(self.shared.tasks.wait)
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
            return $.Deferred(function(deferred) {
                api.post('storagerouters/' + self.data.storageRouter().guid() + '/get_metadata')
                    .then(self.shared.tasks.wait)
                    .then(function(metadata) {
                        self.data.currentUsage(metadata.partitions);
                        self.data.roles([]);
                        $.each(metadata.partitions, function(role, partitions) {
                            $.each(partitions, function(index, partition) {
                                if (partition.guid === self.data.partition().guid()) {
                                    self.data.roles.push({name: role.toLowerCase()});
                                }
                            })
                        });
                    })
                    .done(deferred.resolve)
                    .fail(deferred.reject);
            }).promise();
        };
    };
});
