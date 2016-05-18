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
    'jquery', 'knockout', 'ovs/api', 'ovs/generic', './data', '../../containers/vpool', '../../containers/storagerouter'
], function($, ko, api, generic, data, VPool, StorageRouter) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data = data;
        self.loadStorageRoutersHandle = undefined;
        self.loadvPoolsHandle = undefined;

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, showErrors = false, reasons = [], fields = [];
            if (self.data.name() === undefined || self.data.name() === '' || !self.data.name.valid()) {
                valid = false;
                fields.push('name');
                reasons.push($.t('ovs:wizards.add_vdisk.gather.invalid_name'));
            }
            if (self.data.vPool() === undefined) {
                valid = false;
                fields.push('vpool');
                reasons.push($.t('ovs:wizards.add_vdisk.gather.invalid_vpool'));
            }
            if (self.data.name() === undefined) {
                valid = false;
                fields.push('storageouter');
                reasons.push($.t('ovs:wizards.add_vdisk.gather.invalid_storagerouter'));
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
                        contents: 'storagedrivers,vpools_guids',
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
