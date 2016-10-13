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
    '../../containers/storagerouter', './data'
], function($, ko, api, generic, shared, StorageRouter, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared = shared;
        self.data = data;

        // Observables
        self.loading = ko.observable(false);

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, reasons = [], fields = [];
            if (self.data.storageRouter() === undefined) {
                valid = false;
                fields.push('sr');
                reasons.push($.t('ovs:wizards.scrub.gather.nostoragerouter'));
            }
            return { value: valid, reasons: reasons, fields: fields };
        });

        // Functions
        self.finish = function() {
            return $.Deferred(function(deferred) {
                var data = {
                    storagerouter_guid: self.data.storageRouter().guid()
                };
                api.post('vdisks/' + self.data.vDisk().guid() + '/scrub', { data: data })
                    .then(function(taskID) {
                        generic.alertInfo(
                            $.t('ovs:wizards.scrub.gather.scrubstarted'),
                            $.t('ovs:wizards.scrub.gather.inprogress')
                        );
                        deferred.resolve();
                        return taskID;
                    })
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        generic.alertSuccess(
                            $.t('ovs:generic.finished'),
                            $.t('ovs:wizards.scrub.gather.success')
                        );
                    })
                    .fail(function(error) {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.scrub.gather.failed')
                        );
                        deferred.resolve(error);
                    });
            }).promise();
        };

        // Durandal
        self.activate = function() {
            self.loading(true);
            return $.Deferred(function(deferred) {
                api.get('vdisks/' + self.data.vDisk().guid() + '/get_scrub_storagerouters', {
                    queryparams: {
                        contents: '',
                        sort: 'name'
                    }
                })
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
                    .fail(deferred.reject)
                    .always(function () {
                        self.loading(false);
                    });
            }).promise();
        };
    };
});
