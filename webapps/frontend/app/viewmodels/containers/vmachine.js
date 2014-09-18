// Copyright 2014 CloudFounders NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define([
    'jquery', 'knockout',
    'ovs/generic', 'ovs/api', 'ovs/shared',
    'viewmodels/containers/vdisk'
], function($, ko, generic, api, shared, VDisk) {
    "use strict";
    return function(guid) {
        var self = this;

        // Variables
        self.shared               = shared;
        self.storageApplinceGuids = [];
        self.vPoolGuids           = [];
        self.vMachineGuids        = [];

        // Handles
        self.loadVDisksHandle      = undefined;
        self.loadStorageRouterGuid = undefined;
        self.loadHandle            = undefined;
        self.loadVpoolGuid         = undefined;
        self.loadChildrenGuid      = undefined;
        self.loadSChildrenGuid     = undefined;

        // External dependencies
        self.pMachine       = ko.observable();
        self.storageRouters = ko.observableArray([]);
        self.vPools         = ko.observableArray([]);
        self.vMachines      = ko.observableArray([]);

        // Observables
        self.guid                  = ko.observable(guid);
        self.loading               = ko.observable(false);
        self.loaded                = ko.observable(false);
        self.pMachineGuid          = ko.observable();
        self.name                  = ko.observable();
        self.hypervisorStatus      = ko.observable();
        self.ipAddress             = ko.observable();
        self.isVTemplate           = ko.observable();
        self.status                = ko.observable();
        self.configuration         = ko.observable();
        self._configuration        = ko.observable();
        self.iops                  = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.storedData            = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.cacheHits             = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.cacheMisses           = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.readSpeed             = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.writeSpeed            = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.backendReads          = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.backendWritten        = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.backendRead           = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.bandwidthSaved        = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.failoverMode          = ko.observable();
        self.snapshots             = ko.observableArray([]);
        self.vDisks                = ko.observableArray([]);
        self.templateChildrenGuids = ko.observableArray([]);
        self.cacheStrategies       = ko.observableArray([undefined, { name: 'onread' }, { name: 'onwrite' }, { name: 'none' }]);
        self.hasFOC                = ko.observableArray([undefined, { value: true }, { value: false }]);

        // Computed
        self.cacheRatio = ko.computed(function() {
            if (self.cacheHits() === undefined || self.cacheMisses() === undefined) {
                return undefined;
            }
            var total = (self.cacheHits.raw() || 0) + (self.cacheMisses.raw() || 0);
            if (total === 0) {
                total = 1;
            }
            return generic.formatRatio((self.cacheHits.raw() || 0) / total * 100);
        });
        self.isRunning = ko.computed(function() {
            return self.hypervisorStatus() === 'RUNNING';
        });
        self.bandwidth = ko.computed(function() {
            if (self.readSpeed() === undefined || self.writeSpeed() === undefined) {
                return undefined;
            }
            var total = (self.readSpeed.raw() || 0) + (self.writeSpeed.raw() || 0);
            return generic.formatSpeed(total);
        });
        self.configIops = ko.computed({
            read: function() {
                if (self._configuration() !== undefined && self._configuration().hasOwnProperty('iops')) {
                    return self._configuration().iops;
                }
                return undefined;
            },
            write: function(value) {
                var target = self._configuration();
                if (value === '') {
                    delete target.iops;
                } else {
                    target.iops = parseInt(value, 10);
                    if (isNaN(target.iops)) {
                        delete target.iops;
                    }
                }
                self._configuration(target);
            }
        }).extend({ notify: 'always' });
        self.configCacheStrategy = ko.computed({
            read: function() {
                if (self._configuration() !== undefined && self._configuration().hasOwnProperty('cache_strategy')) {
                    return { name: self._configuration().cache_strategy };
                }
                return undefined;
            },
            write: function(value) {
                var target = self._configuration();
                if (value === undefined) {
                    delete target.cache_strategy;
                } else {
                    target.cache_strategy = value.name;
                }
                self._configuration(target);
            }
        });
        self.configCacheSize = ko.computed({
            read: function() {
                if (self._configuration() !== undefined && self._configuration().hasOwnProperty('cache_size')) {
                    return self._configuration().cache_size;
                }
                return undefined;
            },
            write: function(value) {
                var target = self._configuration();
                if (value === '') {
                    delete target.cache_size;
                } else {
                    target.cache_size = parseInt(value, 10);
                    if (isNaN(target.cache_size)) {
                        delete target.cache_size;
                    }
                }
                self._configuration(target);
            }
        }).extend({ notify: 'always' });
        self.configFoc = ko.computed({
            read: function() {
                if (self._configuration() !== undefined && self._configuration().hasOwnProperty('foc')) {
                    return { value: self._configuration().foc };
                }
                return undefined;
            },
            write: function(value) {
                var target = self._configuration();
                if (value === undefined) {
                    delete target.foc;
                } else {
                    target.foc = value.value;
                }
                self._configuration(target);
            }
        });
        self.configChanged = ko.computed(function() {
            var changed = false;
            if (self._configuration() !== undefined && self.configuration() !== undefined) {
                $.each(['iops', 'cache_strategy', 'cache_size', 'foc'], function (i, key) {
                    if (self._configuration()[key] !== self.configuration()[key]) {
                        changed = true;
                        return false;
                    }
                });
            }
            return changed;
        });

        // Functions
        self.fetchTemplateChildrenGuids = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadChildrenGuid)) {
                    self.loadChildrenGuid = api.get('vmachines/' + self.guid() + '/get_children')
                        .done(function(data) {
                            self.templateChildrenGuids(data);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.fillData = function(data) {
            generic.trySet(self.name, data, 'name');
            generic.trySet(self.hypervisorStatus, data, 'hypervisor_status');
            generic.trySet(self.storedData, data, 'stored_data');
            generic.trySet(self.ipAddress, data, 'ip');
            generic.trySet(self.isVTemplate, data, 'is_vtemplate');
            generic.trySet(self.snapshots, data, 'snapshots');
            generic.trySet(self.status, data, 'status', generic.lower);
            generic.trySet(self.failoverMode, data, 'failover_mode', generic.lower);
            generic.trySet(self.pMachineGuid, data, 'pmachine_guid');
            if (data.hasOwnProperty('configuration')) {
                if (self._configuration() === undefined) {
                    self._configuration($.extend({}, data.configuration));
                }
                if (self.configuration() === undefined) {
                    self.configuration($.extend({}, data.configuration));
                }
                var target = self._configuration();
                generic.merge(self.configuration(), data.configuration, target, ['iops', 'cache_strategy', 'cache_size', 'foc']);
                self._configuration(target);
                self.configuration($.extend({}, data.configuration));
            }
            if (data.hasOwnProperty('storagerouters_guids')) {
                self.storageRouterGuids = data.storagerouters_guids;
            }
            if (data.hasOwnProperty('vpools_guids')) {
                self.vPoolGuids = data.vpools_guids;
            }
            if (data.hasOwnProperty('vdisks_guids')) {
                generic.crossFiller(
                    data.vdisks_guids, self.vDisks,
                    function(guid) {
                        var vd = new VDisk(guid);
                        vd.loading(true);
                        return vd;
                    }, 'guid'
                );
            }
            if (data.hasOwnProperty('statistics')) {
                var stats = data.statistics;
                self.iops(stats.write_operations_ps + stats.read_operations_ps);
                self.cacheHits(stats.sco_cache_hits_ps + stats.cluster_cache_hits_ps);
                self.cacheMisses(stats.sco_cache_misses_ps);
                self.readSpeed(stats.data_read_ps);
                self.writeSpeed(stats.data_written_ps);
                self.backendWritten(stats.backend_data_written);
                self.backendRead(stats.backend_data_read);
                self.backendReads(stats.sco_cache_hits + stats.cluster_cache_hits);
                self.bandwidthSaved(Math.max(0, stats.data_read - stats.backend_data_read));
            }

            self.snapshots.sort(function(a, b) {
                // Newest first
                return b.timestamp - a.timestamp;
            });

            self.loaded(true);
            self.loading(false);
        };
        self.load = function(contents) {
            return $.Deferred(function(deferred) {
                self.loading(true);
                if (generic.xhrCompleted(self.loadHandle)) {
                    var options = {};
                    if (contents !== undefined) {
                        options.contents = contents;
                    }
                    self.loadHandle = api.get('vmachines/' + self.guid(), { queryparams: options })
                        .done(function(data) {
                            self.fillData(data);
                            self.loaded(true);
                            deferred.resolve();
                        })
                        .fail(deferred.reject)
                        .always(function() {
                            self.loading(false);
                        });
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
    };
});
