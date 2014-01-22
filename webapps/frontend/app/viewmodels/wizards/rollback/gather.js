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
    'ovs/api', 'ovs/shared', 'ovs/generic',
    '../../containers/vmachine', '../../containers/vdisk', './data'
], function($, ko, api, shared, generic, VMachine, VDisk, data) {
    "use strict";
    return function() {
        var self = this;

        self.data   = data;
        self.shared = shared;

        self.canContinue = ko.computed(function() {
            if (self.data.velement() === undefined) {
                return {value: false, reason: $.t('ovs:wizards.rollback.gather.no' + self.data.type)};
            }
            if (self.data.velement().snapshots() === undefined || self.data.velement().snapshots().length === 0) {
                return {value: false, reason: $.t('ovs:wizards.rollback.gather.nosnapshots')};
            }
            return {value: true, reason: undefined};
        });

        self.finish = function() {
            return $.Deferred(function(deferred) {
                var data = {
                    timestamp: self.data.snapshot().timestamp
                };
                api.post(self.data.type + 's/' + self.data.velement().guid() + '/rollback', data)
                    .then(function(taskID) {
                        generic.alertInfo(
                            $.t('ovs:wizards.rollback.gather.rollbackstarted'),
                            $.t('ovs:wizards.rollback.gather.inprogress' + self.data.type, { what: self.data.velement().name() })
                        );
                        deferred.resolve();
                        return taskID;
                    })
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        generic.alertSuccess(
                            $.t('ovs:generic.finished'),
                            $.t('ovs:wizards.rollback.gather.success' + self.data.type, { what: self.data.velement().name() })
                        );
                    })
                    .fail(function() {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.rollback.gather.failed' + self.data.type, { what: self.data.velement().name() })
                        );
                        deferred.resolve(false);
                    });
            }).promise();
        };

        self.activate = function() {
            if (self.data.velement() === undefined || self.data.velement().guid() !== self.data.guid()) {
                if (self.data.type === 'vmachine') {
                    self.data.velement(new VMachine(self.data.guid()));
                    self.data.snapshot(undefined);
                } else {
                    self.data.velement(new VDisk(self.data.guid()));
                    self.data.snapshot(undefined);
                }
            }
            self.data.velement().load();
        };
    };
});
