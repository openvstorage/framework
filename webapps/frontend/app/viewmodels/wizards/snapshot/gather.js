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
            if (self.data.vm() === undefined) {
                return {value: false, reason: $.t('ovs:wizards.snapshot.gather.nomachine')};
            }
            if (!self.data.name()) {
                return {value: false, reason: $.t('ovs:wizards.snapshot.gather.noname')};
            }
            return {value: true, reason: undefined};
        });

        // Functions
        self.finish = function() {
            return $.Deferred(function(deferred) {
                var data = {
                    name: self.data.name(),
                    consistent: self.data.isConsistent()
                };
                api.post('vmachines/' + self.data.vm().guid() + '/snapshot', data)
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
                    .fail(function() {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.snapshot.confirm.failed', { what: self.data.vm().name() })
                        );
                        deferred.resolve(false);
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
