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
