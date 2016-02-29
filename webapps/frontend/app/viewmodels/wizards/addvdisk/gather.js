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
    'jquery', 'knockout', 'ovs/api', 'ovs/generic', './data', '../../containers/vpool', '../../containers/storagerouter'
], function($, ko, api, generic, data, VPool, StorageRouter) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data   = data;
        self.loadStorageRoutersHandle = undefined;
        self.loadvPoolsHandle = undefined;

        // Observables

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, showErrors = false, reasons = [], fields = [];
            if (self.data.name() === undefined || self.data.name() === '' || !self.data.name.valid()) {
                valid = false;
                fields.push('name');
                reasons.push($.t('ovs:wizards.add_vdisk.gather.invalid_name'));
            }
            return { value: valid, showErrors: showErrors, reasons: reasons, fields: fields };
        });

        self.activate = function() {
            generic.xhrAbort(self.loadVPoolsHandle);
            if (generic.xhrCompleted(self.loadVPoolsHandle)) {
                self.loadVPoolsHandle = api.get('vpools', {
                        queryparams: {
                        contents: ''
                    }
                })
                    .done(function (data) {
                        var guids = [], vpData = {};
                        $.each(data.data, function (index, item) {
                            guids.push(item.guid);
                            vpData[item.guid] = item;
                        });
                        generic.crossFiller(
                            guids, self.data.vPools,
                            function (guid) {
                                return new VPool(guid);
                            }, 'guid'
                        );
                        $.each(self.data.vPools(), function (index, vpool) {
                            if (guids.contains(vpool.guid())) {
                                vpool.fillData(vpData[vpool.guid()]);
                            }
                        });
                    });
                self.loadStorageRoutersHandle = api.get('storagerouters', {
                    queryparams: {
                        contents: 'storagedrivers,vpools,vpools_guids',
                        sort: 'name'
                    }
                })
                    .done(function(data) {
                        var guids = [], srdata = {};
                        $.each(data.data, function(index, item) {
                            guids.push(item.guid);
                            srdata[item.guid] = item;
                        });
                        generic.crossFiller(
                            guids, self.data.storageRouters,
                            function(guid) {
                                return new StorageRouter(guid);
                            }, 'guid'
                        );
                        $.each(self.data.storageRouters(), function(index, storageRouter) {
                            if (guids.contains(storageRouter.guid())) {
                                storageRouter.fillData(srdata[storageRouter.guid()]);
                            }
                        });
                    });
            }
        };
        self.finish = function() {
            return $.Deferred(function(deferred) {
                generic.alertInfo(
                    $.t('ovs:wizards.add_vdisk.gather.started'),
                    $.t('ovs:wizards.add_vdisk.gather.inprogress')
                );
                api.post('vdisks', {
                    data: {
                        devicename: self.data.name(),
                        size: self.data.size(),
                        vpool_guid: self.data.vPool().guid(),
                        storagerouter_guid: self.data.storageRouter().guid()
                    }
                })
                    .done(function() {
                        generic.alertSuccess(
                            $.t('ovs:wizards.add_vdisk.gather.complete'),
                            $.t('ovs:wizards.add_vdisk.gather.success')
                        );
                    })
                    .fail(function(error) {
                        error = $.parseJSON(error.responseText);
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.add_vdisk.gather.failed')
                        );
                    });
                deferred.resolve();
            }).promise();
        };
    };
});
