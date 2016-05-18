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
    './data', 'ovs/shared', 'ovs/generic', 'ovs/api'
], function($, ko, data, shared, generic, api) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared = shared;
        self.data   = data;

        // Computed
        self.canContinue = ko.observable({ value: true, reasons: [], fields: [] });

        // Functions
        self.finish = function() {
            return $.Deferred(function(deferred) {
                generic.alertInfo(
                    $.t('ovs:wizards.clone.confirm.clonestarted'),
                    $.t('ovs:wizards.clone.confirm.inprogress', { what: self.data.vDisk().name() })
                );
                var data = {
                    name: self.data.name(),
                    storagerouter_guid: self.data.storageRouter().guid()
                };
                if (self.data.snapshot() !== undefined) {
                    data.snapshot_id = self.data.snapshot().guid;
                }
                api.post('vdisks/' + self.data.vDisk().guid() + '/clone', { data: data })
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        generic.alertSuccess(
                            $.t('ovs:generic.finished'),
                            $.t('ovs:wizards.clone.confirm.success',{ what: self.data.vDisk().name() })
                        );
                    })
                    .fail(function(error) {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.clone.confirm.failed', { what: self.data.vDisk().name() })
                        );
                    });
                deferred.resolve();
            }).promise();
        };
    };
});
