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
    'ovs/api', 'ovs/generic',
    '../../containers/storagerouter', './data'
], function($, ko, api, generic, StorageRouter, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data = data;

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, reasons = [], fields = [];
            if (self.data.storageRouter() === undefined) {
                valid = false;
                fields.push('vm');
                reasons.push($.t('ovs:wizards.clone.gather.nostoragerouter'));
            }
            if (!self.data.name()) {
                valid = false;
                fields.push('name');
                reasons.push($.t('ovs:wizards.clone.gather.noname'));
            }
            return { value: valid, reasons: reasons, fields: fields };
        });

        // Durandal
        self.activate = function() {
            return $.Deferred(function(deferred) {
                var options = {
                    sort: 'name',
                    contents: 'vpools_guids'
                };
                api.get('storagerouters', { queryparams: options })
                    .done(function(data) {
                        var guids = [], sadata = {};
                        $.each(data.data, function(index, item) {
                            guids.push(item.guid);
                            sadata[item.guid] = item;
                        });
                        generic.crossFiller(
                            guids, self.data.storageRouters,
                            function(guid) {
                                return new StorageRouter(guid);
                            }, 'guid'
                        );
                        $.each(self.data.storageRouters(), function(index, storageRouter) {
                            if (guids.contains(storageRouter.guid())) {
                                storageRouter.fillData(sadata[storageRouter.guid()]);
                            }
                        });
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };
    };
});
