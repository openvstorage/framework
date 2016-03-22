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
    'knockout', 'jquery',
    'ovs/shared', 'ovs/generic', 'ovs/api', 'ovs/refresher',
    '../containers/vmachine', '../containers/vpool', '../containers/storagerouter', '../containers/failuredomain'
], function(ko, $, shared, generic, api, Refresher, VMachine, VPool, StorageRouter, FailureDomain) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared    = shared;
        self.guard     = { authenticated: true, registered: true };
        self.refresher = new Refresher();
        self.topItems  = 10;
        self.query     = {
            type: 'AND',
            items: [['is_vtemplate', 'EQUALS', false],
                    ['status', 'NOT_EQUALS', 'CREATED']]
        };

        // Handles
        self.loadStorageRoutersHandle = undefined;
        self.loadVPoolsHandle         = undefined;
        self.loadFailureDomainsHandle = undefined;

        // Observ ables
        self.storageRoutersLoading = ko.observable(false);
        self.failureDomainsLoading = ko.observable(false);
        self.vPoolsLoading         = ko.observable(false);
        self.storageRouters        = ko.observableArray([]);
        self.vPools                = ko.observableArray([]);
        self.failureDomains        = ko.observableArray([]);
        self.amountOfVMachines     = ko.observable(0);
        self.topVMachines          = ko.observableArray([]);

        // Computed
        self._cacheRatio = ko.computed(function() {
            var hits = 0, misses = 0, total, raw;
            $.each(self.vPools(), function(index, vpool) {
                hits += (vpool.cacheHits.raw() || 0);
                misses += (vpool.cacheMisses.raw() || 0);
            });
            total = hits + misses;
            if (total === 0) {
                total = 1;
            }
            raw = hits / total * 100;
            return {
                value: generic.formatRatio(raw),
                raw: raw
            };
        });
        self.cacheRatio = ko.computed(function() {
            return self._cacheRatio().value;
        });
        self.cacheRatio.raw = ko.computed(function() {
            return self._cacheRatio().raw;
        });
        self._iops = ko.computed(function() {
            var total = 0;
            $.each(self.vPools(), function(index, vpool) {
                total += (vpool.iops.raw() || 0);
            });
            return {
                value: generic.formatNumber(total),
                raw: total
            };
        });
        self.iops = ko.computed(function() {
            return self._iops().value;
        });
        self.iops.raw = ko.computed(function() {
            return self._iops().raw;
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
                var guid = storageRouter.primaryFailureDomainGuid(),
                    color = storageRouter.statusColor();
                if (guid !== undefined) {
                    if (!dataset.hasOwnProperty(guid)) {
                        dataset[guid] = {
                            green: 0,
                            orange: 0,
                            red: 0,
                            lightgrey: 0
                        };
                    }
                    dataset[guid][color] += 1
                }
            });
            return dataset;
        });

        // Functions
        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loadStorageRouters()
                    .then(self.loadFailureDomains)
                    .then(self.loadVPools)
                    .done(deferred.resolve)
                    .fail(deferred.reject);
            }).promise();
        };
        self.loadFailureDomains = function() {
            return $.Deferred(function(deferred) {
                self.failureDomainsLoading(true);
                if (generic.xhrCompleted(self.loadFailureDomainsHandle)) {
                    self.loadFailureDomainsHandle = api.get('failure_domains', {
                        queryparams: { contents: 'name', sort: 'name' }
                    })
                        .done(function(data) {
                            var guids = [], fdata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                fdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.failureDomains,
                                function(guid) {
                                    return new FailureDomain(guid);
                                }, 'guid'
                            );
                            $.each(self.failureDomains(), function(index, failureDomain) {
                                failureDomain.fillData(fdata[failureDomain.guid()]);
                            });
                            deferred.resolve();
                        })
                        .fail(deferred.reject)
                        .always(function() {
                            self.failureDomainsLoading(false);
                        });
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.loadVPools = function() {
            return $.Deferred(function(deferred) {
                self.vPoolsLoading(true);
                if (generic.xhrCompleted(self.loadVPoolsHandle)) {
                    var filter = {
                        contents: 'statistics,stored_data'
                    };
                    self.loadVPoolsHandle = api.get('vpools', { queryparams: filter })
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
                    self.loadStorageRoutersHandle = api.get('storagerouters', {
                        queryparams: {
                            contents: 'status,primary_failure_domain'
                        }
                    })
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
            self.shared.footerData(self.vPools);
        };
        self.deactivate = function() {
            $.each(shared.hooks.dashboards, function(index, dashboard) {
                dashboard.activator.deactivateItem(dashboard.module);
            });
            self.refresher.stop();
            self.shared.footerData(ko.observable());
        };
    };
});
