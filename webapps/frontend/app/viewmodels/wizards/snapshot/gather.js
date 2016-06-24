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
    'ovs/api', 'ovs/generic', 'ovs/shared',
    '../../containers/vmachine', '../../containers/vdisk', './data'
], function($, ko, api, generic, shared, VMachine, VDisk, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared = shared;
        self.data = data;

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, reasons = [], fields = [];
            if (self.data.vObject() === undefined) {
                valid = false;
                reasons.push($.t('ovs:wizards.snapshot.gather.noobject'));
            } else if (!self.data.name()) {
                valid = false;
                fields.push('name');
                reasons.push($.t('ovs:wizards.snapshot.gather.noname'));
            }
            return { value: valid, reasons: reasons, fields: fields };
        });

        // Functions
        self.finish = function() {
            return $.Deferred(function(deferred) {
                var data = {
                    name: self.data.name(),
                    consistent: self.data.isConsistent(),
                    sticky: self.data.isSticky()
                };
                var call;
                if (self.data.mode() === 'vmachine') {
                    call = api.post('vmachines/' + self.data.guid() + '/snapshot', { data: data })
                } else {
                    call = api.post('vdisks/' + self.data.guid() + '/create_snapshot', { data: data })
                }
                call.then(function(taskID) {
                        generic.alertInfo(
                            $.t('ovs:wizards.snapshot.confirm.snapshotstarted'),
                            $.t('ovs:wizards.snapshot.confirm.inprogress')
                        );
                        deferred.resolve();
                        return taskID;
                    })
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        generic.alertSuccess(
                            $.t('ovs:generic.finished'),
                            $.t('ovs:wizards.snapshot.confirm.success')
                        );
                    })
                    .fail(function(error) {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.snapshot.confirm.failed')
                        );
                        deferred.resolve(error);
                    });
            }).promise();
        };

        // Durandal
        self.activate = function() {
            if (self.data.vObject() === undefined || self.data.vObject().guid() !== self.data.guid()) {
                if (self.data.mode() === 'vmachine') {
                    self.data.vObject(new VMachine(self.data.guid()));
                } else {
                    self.data.vObject(new VDisk(self.data.guid()));
                }
                self.data.vObject()
                    .load()
                    .done(function() {
                        if (self.data.name() === '' && self.data.mode() === 'vmachine') {
                            self.data.name(self.data.vObject().name() + '-snapshot');
                        }
                    });
            }
        };
    };
});
