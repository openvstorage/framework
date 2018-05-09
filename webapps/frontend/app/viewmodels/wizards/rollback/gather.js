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
    'viewmodels/containers/vdisk/vdisk',
    './data'
], function($, ko, api, shared, generic, VDisk, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data   = data;
        self.shared = shared;

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, reasons = [], fields = [];
            if (self.data.velement() === undefined) {
                valid = false;
                fields.push('velement');
                reasons.push($.t('ovs:wizards.rollback.gather.no' + self.data.type));
            } else if (self.data.velement().snapshots() === undefined || self.data.velement().snapshots().length === 0) {
                valid = false;
                fields.push('snapshots');
                reasons.push($.t('ovs:wizards.rollback.gather.nosnapshots'));
            }
            return { value: valid, reasons: reasons, fields: fields };
        });

        // Functions
        self.finish = function() {
            return $.Deferred(function(deferred) {
                var data = {
                    timestamp: self.data.snapshot().timestamp
                };
                api.post('vdisks/' + self.data.velement().guid() + '/rollback', { data: data })
                    .then(function(taskID) {
                        generic.alertInfo(
                            $.t('ovs:wizards.rollback.gather.rollbackstarted'),
                            $.t('ovs:wizards.rollback.gather.inprogress', { what: self.data.velement().name() })
                        );
                        deferred.resolve();
                        return taskID;
                    })
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        generic.alertSuccess(
                            $.t('ovs:generic.finished'),
                            $.t('ovs:wizards.rollback.gather.success', { what: self.data.velement().name() })
                        );
                    })
                    .fail(function(error) {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.rollback.gather.failed', { what: self.data.velement().name() })
                        );
                        deferred.resolve(error);
                    });
            }).promise();
        };

        // Durandal
        self.activate = function() {
            if (self.data.velement() === undefined || self.data.velement().guid() !== self.data.guid()) {
                self.data.velement(new VDisk(self.data.guid()));
                self.data.snapshot(undefined);
            }
            self.data.velement().load();
        };
    };
});
