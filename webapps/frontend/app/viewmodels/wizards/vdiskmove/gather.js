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
    './data',
    'viewmodels/containers/storagerouter/storagerouter'
], function($, ko, api, generic, shared, data, StorageRouter) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data   = data;
        self.shared = shared;

        // Observables
        self.loading = ko.observable(false);

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, reasons = [], fields = [];
            if (self.data.targets().length === 0) {
                valid = false;
                fields.push('targets');
                reasons.push($.t('ovs:wizards.vdisk_move.gather.targets_unavailable_message'));
            }
            if (self.data.source() === undefined) {
                valid = false;
                fields.push('source');
                reasons.push($.t('ovs:wizards.vdisk_move.gather.source_unknown_message'));
            }
            return { value: valid, reasons: reasons, fields: fields };
        });

        // Functions
        self.finish = function() {
            return $.Deferred(function(deferred) {
                api.post('vdisks/' + self.data.vDisk().guid() + '/move', { data: { target_storagerouter_guid: self.data.target().guid() } })
                    .then(function(taskID) {
                        generic.alertInfo(
                            $.t('ovs:wizards.vdisk_move.confirm.started'),
                            $.t('ovs:wizards.vdisk_move.confirm.in_progress', { what : self.data.vDisk().name() })
                        );
                        deferred.resolve();
                        return taskID;
                    })
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        generic.alertSuccess(
                            $.t('ovs:generic.finished'),
                            $.t('ovs:wizards.vdisk_move.confirm.success', { what : self.data.vDisk().name() })
                        );
                    })
                    .fail(function(error) {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.vdisk_move.confirm.failed', { what : self.data.vDisk().name(), why: generic.extractErrorMessage(error) })
                        );
                        deferred.reject();
                    });
            }).promise();
        };

        // Durandal
        self.activate = function() {
            self.loading(true);
            return $.Deferred(function(deferred) {
                api.get('vdisks/' + self.data.vDisk().guid() + '/get_target_storagerouters', {
                    queryparams: {
                        contents: '',
                        sort: 'name'
                    }
                })
                    .done(function(data) {
                        var guids = [], sadata = {}, targets = [];
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
                            if (self.data.vDisk().storageRouterGuid() === storageRouter.guid()) {
                                self.data.source(storageRouter);
                            } else {
                                targets.push(storageRouter);
                            }
                        });
                        self.data.targets(targets);
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
