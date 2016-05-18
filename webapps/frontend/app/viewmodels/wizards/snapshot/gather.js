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
    '../../containers/vmachine', './data'
], function($, ko, api, generic, shared, VMachine, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared = shared;
        self.data = data;

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, reasons = [], fields = [];
            if (self.data.vm() === undefined) {
                valid = false;
                fields.push('vm');
                reasons.push($.t('ovs:wizards.snapshot.gather.nomachine'));
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
                api.post('vmachines/' + self.data.vm().guid() + '/snapshot', { data: data })
                    .then(function(taskID) {
                        generic.alertInfo(
                            $.t('ovs:wizards.snapshot.confirm.snapshotstarted'),
                            $.t('ovs:wizards.snapshot.confirm.inprogress', { what: self.data.vm().name() })
                        );
                        deferred.resolve();
                        return taskID;
                    })
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        generic.alertSuccess(
                            $.t('ovs:generic.finished'),
                            $.t('ovs:wizards.snapshot.confirm.success', { what: self.data.vm().name() })
                        );
                    })
                    .fail(function(error) {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.snapshot.confirm.failed', { what: self.data.vm().name() })
                        );
                        deferred.resolve(error);
                    });
            }).promise();
        };

        // Durandal
        self.activate = function() {
            if (self.data.vm() === undefined || self.data.vm().guid() !== self.data.machineGuid()) {
                self.data.vm(new VMachine(self.data.machineGuid()));
                self.data.vm()
                    .load()
                    .done(function() {
                        self.data.name(self.data.vm().name() + '-snapshot');
                    });
            }
        };
    };
});
