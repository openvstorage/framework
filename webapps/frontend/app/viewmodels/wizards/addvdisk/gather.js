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
    'jquery', 'knockout', 'ovs/api', 'ovs/generic', 'ovs/shared', './data', '../../containers/vpool', '../../containers/storagerouter'
], function($, ko, api, generic, shared, data, VPool, StorageRouter) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data   = data;
        self.shared = shared;

        // Handles
        self.loadStorageRoutersHandle = undefined;
        self.loadvPoolsHandle         = undefined;

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, showErrors = false, reasons = [], fields = [], maxSize = self.data.sizeEntry.max * Math.pow(1024, 3);
            if (self.data.name() === '') {
                valid = false;
                fields.push('name');
                reasons.push($.t('ovs:wizards.add_vdisk.gather.invalid_name'));
            }
            if (self.data.vPool() === undefined) {
                valid = false;
                fields.push('vpool');
                reasons.push($.t('ovs:wizards.add_vdisk.gather.invalid_vpool'));
            }
            if (self.data.storageRouter() === undefined) {
                valid = false;
                fields.push('storageouter');
                reasons.push($.t('ovs:wizards.add_vdisk.gather.invalid_storagerouter'));
            }
            if (self.data.size() > maxSize) {
                valid = false;
                fields.push('size');
                reasons.push($.t('ovs:wizards.add_vdisk.gather.invalid_size', {amount: parseInt(self.data.sizeEntry.max / 1024), unit: $.t('ovs:generic.units.tib')}));
            }
            return { value: valid, showErrors: showErrors, reasons: reasons, fields: fields };
        });
        self.cleanedName = ko.computed(function() {
            var cleaned, extension='';
            cleaned = self.data.name().replace(/^(\/)+|(\/)+$/g, '').replace(/ /g,"_").toLowerCase();
            cleaned = cleaned.replace(/[^a-z0-9-_\.\/]+/g, "");
            while (cleaned.indexOf('//') > -1) {
                cleaned = cleaned.replace(/\/\//g, '/');
            }
            if (cleaned.indexOf('.') > -1) {
                extension = cleaned.split('.').pop();
                if (extension.length == 3 || extension.length == 4) {
                    return cleaned
                }
                if (extension.length === 0) {
                   return cleaned + 'raw';
                }
            }
            return cleaned + '.raw';
        });

        // Durandal
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
                        contents: 'vpools_guids',
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
                    $.t('ovs:wizards.add_vdisk.gather.in_progress')
                );
                deferred.resolve();
                api.post('vdisks', {
                    data: {
                        name: self.data.name(),
                        size: self.data.size(),
                        vpool_guid: self.data.vPool().guid(),
                        storagerouter_guid: self.data.storageRouter().guid()
                    }
                })
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        generic.alertSuccess(
                            $.t('ovs:wizards.add_vdisk.gather.complete'),
                            $.t('ovs:wizards.add_vdisk.gather.success')
                        );
                    })
                    .fail(function(error) {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.add_vdisk.gather.failed', {why: error})
                        );
                    });
            }).promise();
        };
    };
});
