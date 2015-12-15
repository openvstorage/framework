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
    '../../containers/storagerouter', '../../containers/vdisk', './data'
], function($, ko, api, generic, StorageRouter, VDisk, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data = data;

        // Observables
        self.loaded  = ko.observable(false);
        self.loading = ko.observable(false);

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, reasons = [], fields = [];
            if (self.data.storageRouter() === undefined) {
                valid = false;
                fields.push('vm');
                reasons.push($.t('ovs:wizards.clone.gather.nostoragerouter'));
            }
            if (!self.data.name.valid()) {
                valid = false;
                fields.push('name');
                reasons.push($.t('ovs:wizards.clone.gather.invalid_name'));
            }
            $.each(self.data.vDisks(), function(index, vdisk) {
                if (self.data.name() === vdisk.name()) {
                    valid = false;
                    fields.push('name');
                    reasons.push($.t('ovs:wizards.clone.gather.duplicate_name'));
                }
            });
            return { value: valid, reasons: reasons, fields: fields };
        });

        // Durandal
        self.activate = function() {
            self.loading(true);
            var calls = [];
            calls.push($.Deferred(function(deferred) {
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
            }).promise());
            calls.push($.Deferred(function(deferred) {
                var options = {
                    contents: ''
                };
                api.get('vdisks', { queryparams: options })
                    .done(function (data) {
                        var guids = [], vdiskData = {};
                        $.each(data.data, function (index, item) {
                            guids.push(item.guid);
                            vdiskData[item.guid] = item;
                        });
                        generic.crossFiller(
                            guids, self.data.vDisks,
                            function (guid) {
                                return new VDisk(guid);
                            }, 'guid'
                        );
                        $.each(self.data.vDisks(), function (index, vdisk) {
                            if (guids.contains(vdisk.guid())) {
                                vdisk.fillData(vdiskData[vdisk.guid()]);
                            }
                        });
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise());
            return $.Deferred(function(deferred) {
                $.when.apply($, calls)
                    .done(function () {
                        self.loaded(true);
                        deferred.resolve();
                    })
                    .fail(deferred.reject)
                    .always(function () {
                        self.loading(false);
                    });
            }).promise();
        };
    };
});
