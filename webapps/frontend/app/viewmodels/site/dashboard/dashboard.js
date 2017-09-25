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
    'knockout', 'jquery',
    'ovs/shared', 'ovs/generic', 'ovs/api', 'ovs/refresher',
    '../../containers/vpool', '../../containers/storagerouter'
], function(ko, $, shared, generic, api, Refresher, VPool, StorageRouter) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared    = shared;
        self.guard     = { authenticated: true };
        self.refresher = new Refresher();

        // Handles
        self.loadStorageRoutersHandle = undefined;
        self.loadVPoolsHandle         = undefined;

        // Observables
        self.storageRouters        = ko.observableArray([]);
        self.storageRoutersLoading = ko.observable(false);
        self.vPools                = ko.observableArray([]);
        self.vPoolsLoaded          = ko.observable(false);
        self.vPoolsLoading         = ko.observable(false);

        // Computed
        self.iops = ko.computed(function() {
            var total = 0;
            $.each(self.vPools(), function(index, vpool) {
                total += (vpool.iops.raw() || 0);
            });
            return generic.formatNumber(total);
        });
        self.readSpeed = ko.computed(function() {
            var total = 0;
            $.each(self.vPools(), function(index, vpool) {
                total += (vpool.readSpeed.raw() || 0);
            });
            return generic.formatSpeed(total);
        });
        self.writeSpeed = ko.computed(function() {
            var total = 0;
            $.each(self.vPools(), function(index, vpool) {
                total += (vpool.writeSpeed.raw() || 0);
            });
            return generic.formatSpeed(total);
        });
        self.orderedStorageRouters = ko.computed(function() {
            var dataset = {};
            $.each(self.storageRouters(), function(index, storageRouter) {
                var color = storageRouter.statusColor(),
                    vPoolGuids = storageRouter.vPoolGuids();
                $.each(vPoolGuids, function(_, guid) {
                    if (!dataset.hasOwnProperty(guid)) {
                        dataset[guid] = {
                            green: 0,
                            orange: 0,
                            red: 0,
                            lightgrey: 0
                        };
                    }
                    dataset[guid][color] += 1
                });
            });
            return dataset;
        });

        // Functions
        self.load = function() {
            var calls = [
                self.loadStorageRouters(),
                self.loadVPools()
            ];
            return $.when.apply($, calls);
        };
        self.loadVPools = function() {
            return $.Deferred(function(deferred) {
                self.vPoolsLoading(true);
                if (generic.xhrCompleted(self.loadVPoolsHandle)) {
                    self.loadVPoolsHandle = api.get('vpools', {queryparams: {contents: 'statistics'}})
                        .done(function(data) {
                            var vpools = [], vpool;
                            $.each(data.data, function(index, vpdata) {
                                vpool = new VPool(vpdata.guid);
                                vpool.fillData(vpdata);
                                vpools.push(vpool);
                            });
                            self.vPools(vpools);
                            deferred.resolve();
                        })
                        .fail(deferred.reject)
                        .always(function() {
                            self.vPoolsLoaded(true);
                            self.vPoolsLoading(false);
                        });
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.loadStorageRouters = function() {
            return $.Deferred(function(deferred) {
                self.storageRoutersLoading(true);
                if (generic.xhrCompleted(self.loadStorageRoutersHandle)) {
                    self.loadStorageRoutersHandle = api.get('storagerouters', {queryparams: {contents: 'status,vpools_guids'}})
                        .done(function(data) {
                            var guids = [], sadata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                sadata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.storageRouters,
                                function(guid) {
                                    return new StorageRouter(guid);
                                }, 'guid'
                            );
                            $.each(self.storageRouters(), function(index, storageRouter) {
                                storageRouter.fillData(sadata[storageRouter.guid()]);
                            });
                            deferred.resolve();
                        })
                        .fail(deferred.reject)
                        .always(function() {
                            self.storageRoutersLoading(false);
                        });
                } else {
                    deferred.reject();
                }
            }).promise();
        };

        // Durandal
        self.activate = function() {
            $.each(shared.hooks.dashboards, function(index, dashboard) {
                dashboard.activator.activateItem(dashboard.module);
            });
            self.refresher.init(self.load, 5000);
            self.refresher.run();
            self.refresher.start();
        };
        self.deactivate = function() {
            $.each(shared.hooks.dashboards, function(index, dashboard) {
                dashboard.activator.deactivateItem(dashboard.module);
            });
            self.refresher.stop();
        };
    };
});
