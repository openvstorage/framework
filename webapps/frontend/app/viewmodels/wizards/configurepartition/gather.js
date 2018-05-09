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
    'viewmodels/containers/backend/backend', 'viewmodels/containers/backend/backendtype'
], function($, ko, api, shared, generic, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data   = data;
        self.shared = shared;

        // Observables
        self.loaded  = ko.observable(false);
        self.loading = ko.observable(false);

        // Handles
        self.loadMetadataHandle = undefined;

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = !self.loading(), reasons = [], fields = [];
            if (self.loaded() === false && self.loading() === false) {
                valid = false;
                reasons.push($.t('ovs:wizards.configure_partition.gather.metadata_failure'));
            }
            return { value: valid, reasons: reasons, fields: fields };
        });

        // Functions
        self.isEmpty = generic.isEmpty;
        self.finish = function() {
            return $.Deferred(function(deferred) {
                var roles = [], postData = {
                    size: self.data.partition().size.raw(),
                    offset: self.data.partition().offset.raw(),
                    disk_guid: self.data.disk().guid(),
                    partition_guid: self.data.partition().guid()
                };
                $.each(self.data.roles(), function(index, roleInfo) {
                    roles.push(roleInfo.name.toUpperCase());
                });
                postData.roles = roles;
                api.post('storagerouters/' + self.data.storageRouter().guid() + '/configure_disk', { data: postData })
                        .then(self.shared.tasks.wait)
                        .done(function() {
                            generic.alertSuccess($.t('ovs:generic.saved'), $.t('ovs:wizards.configure_partition.confirm.success'));
                        })
                        .fail(function() {
                            generic.alertError($.t('ovs:generic.error'), $.t('ovs:generic.messages.errorwhile', { what: $.t('ovs:wizards.configure_partition.confirm.creating') }));
                        });
                generic.alertInfo($.t('ovs:wizards.configure_partition.confirm.started'), $.t('ovs:wizards.configure_partition.confirm.in_progress'));
                deferred.resolve();
            }).promise();
        };

        // Durandal
        self.activate = function() {
            self.loading(true);
            generic.xhrAbort(self.loadMetadataHandle);
            self.loadMetadataHandle = api.post('storagerouters/' + self.data.storageRouter().guid() + '/get_metadata')
                .then(self.shared.tasks.wait)
                .done(function(metadata) {
                    if (self.loaded() === false) {
                        self.data.currentUsage(metadata.partitions);
                        self.data.roles([]);
                        $.each(metadata.partitions, function(role, partitions) {
                            $.each(partitions, function(index, partition) {
                                if (partition.guid === self.data.partition().guid()) {
                                    self.data.roles.push({name: role.toLowerCase()});
                                }
                            })
                        });
                    }
                    self.loaded(true);
                })
                .fail(function() {
                    self.loaded(false);
                })
                .always(function() {
                    self.loading(false);
                });
        };
    };
});
