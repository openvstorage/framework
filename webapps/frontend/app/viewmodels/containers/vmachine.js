// Copyright 2014 Open vStorage NV
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
        self.shared                         = shared;
        self.vPoolGuids                     = [];
        self.vMachineGuids                  = [];
        self.configurableStorageDriverAttrs = ['cache_strategy', 'dedupe_mode', 'dtl_enabled', 'dtl_mode', 'sco_size', 'write_buffer'];

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
        self.oldConfiguration      = ko.observable();
        self.newConfiguration      = ko.observable();
        self.iops                  = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.storedData            = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.cacheHits             = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.cacheMisses           = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.readSpeed             = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.writeSpeed            = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.backendWritten        = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.backendRead           = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.bandwidthSaved        = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.failoverMode          = ko.observable();
        self.snapshots             = ko.observableArray([]);
        self.vDisks                = ko.observableArray([]);
        self.templateChildrenGuids = ko.observableArray([]);
        self.cacheStrategies       = ko.observableArray([undefined, { name: 'onread' }, { name: 'onwrite' }, { name: 'none' }]);
        self.dtlModes              = ko.observableArray([undefined, { name: 'nosync' }, { name: 'async' }, { name: 'sync' }]);
        self.dedupeModes           = ko.observableArray([undefined, { name: 'dedupe' }, { name: 'nondedupe' }]);
        self.dtlOptions            = ko.observableArray([undefined, { value: true }, { value: false }]);
        self.scoSizes              = ko.observableArray([undefined, 4, 8, 16, 32, 64, 128]);

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
        self.configCacheStrategy = ko.computed({
            read: function() {
                if (self.newConfiguration() !== undefined && self.newConfiguration().hasOwnProperty('cache_strategy')) {
                    if (self.newConfiguration().cache_strategy === null) {
                        return undefined;
                    }
                    return { name: self.newConfiguration().cache_strategy };
                }
                return undefined;
            },
            write: function(value) {
                var target = self.newConfiguration();
                if (value === undefined) {
                    target.cache_strategy = null;
                } else {
                    target.cache_strategy = value.name;
                }
                self.newConfiguration(target);
            }
        });
        self.dedupeMode = ko.computed({
            read: function() {
                if (self.newConfiguration() !== undefined && self.newConfiguration().hasOwnProperty('dedupe_mode')) {
                    if (self.newConfiguration().dedupe_mode === null) {
                        return undefined;
                    }
                    return { name: self.newConfiguration().dedupe_mode };
                }
                return undefined;
            },
            write: function(value) {
                var target = self.newConfiguration();
                if (value === undefined) {
                    target.dedupe_mode = null;
                } else {
                    target.dedupe_mode = value.name;
                }
                self.newConfiguration(target);
            }
        });
        self.dtlEnable = ko.computed({
            read: function() {
                if (self.newConfiguration() !== undefined && self.newConfiguration().hasOwnProperty('dtl_enabled')) {
                    if (self.newConfiguration().dtl_enabled === null) {
                        return undefined;
                    }
                    return self.newConfiguration().dtl_enabled;
                }
                return undefined;
            },
            write: function(value) {
                var target = self.newConfiguration();
                if (value === undefined) {
                    target.dtl_enabled = null;
                } else {
                    target.dtl_enabled = value;
                }
                self.newConfiguration(target);
            }
        });
        self.dtlMode = ko.computed({
            read: function() {
                if (self.newConfiguration() !== undefined && self.newConfiguration().hasOwnProperty('dtl_mode')) {
                    if (self.newConfiguration().dtl_mode === null) {
                        return undefined;
                    }
                    return { name: self.newConfiguration().dtl_mode };
                }
                return undefined;
            },
            write: function(value) {
                var target = self.newConfiguration();
                if (value === undefined) {
                    target.dtl_mode = null;
                } else {
                    target.dtl_mode = value.name;
                }
                self.newConfiguration(target);
            }
        });
        self.scoSize = ko.computed({
            read: function() {
                if (self.newConfiguration() !== undefined && self.newConfiguration().hasOwnProperty('sco_size')) {
                    if (self.newConfiguration().sco_size === null) {
                        return undefined;
                    }
                    return self.newConfiguration().sco_size;
                }
                return undefined;
            },
            write: function(value) {
                var target = self.newConfiguration();
                if (value === undefined) {
                    target.sco_size = null;
                } else {
                    target.sco_size = value;
                }
                self.newConfiguration(target);
            }
        });
        self.writeBuffer = ko.computed({
            read: function() {
                if (self.newConfiguration() !== undefined && self.newConfiguration().hasOwnProperty('write_buffer')) {
                    if (self.newConfiguration().write_buffer === null) {
                        return undefined;
                    }
                    return self.newConfiguration().write_buffer;
                }
                return undefined;
            },
            write: function(value) {
                var target = self.newConfiguration();
                if (value === '') {
                    target.write_buffer = null;
                } else {
                    target.write_buffer = parseInt(value, 10);
                    if (isNaN(target.write_buffer)) {
                        delete target.write_buffer;
                    } else if (target.write_buffer === 0) {
                        target.write_buffer = null;
                    }
                }
                self.newConfiguration(target);
            }
        }).extend({ notify: 'always' });
        self.configChanged = ko.computed(function() {
            var changed = false;
            if (self.newConfiguration() !== undefined && self.oldConfiguration() !== undefined) {
                $.each(self.configurableStorageDriverAttrs, function (i, key) {
                    if (self.newConfiguration()[key] !== self.oldConfiguration()[key]) {
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
                            self.templateChildrenGuids(data.data);
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
            if (data.hasOwnProperty('snapshots')) {
                var snapshots = [];
                $.each(data.snapshots, function(index, snapshot) {
                    if (snapshot.in_backend) {
                        snapshots.push(snapshot);
                    }
                });
                self.snapshots(snapshots);
            }
            generic.trySet(self.status, data, 'status', generic.lower);
            generic.trySet(self.failoverMode, data, 'failover_mode', generic.lower);
            generic.trySet(self.pMachineGuid, data, 'pmachine_guid');
            if (data.hasOwnProperty('configuration')) {
                if (self.newConfiguration() === undefined) {
                    self.newConfiguration($.extend({}, data.configuration));
                }
                if (self.oldConfiguration() === undefined) {
                    self.oldConfiguration($.extend({}, data.configuration));
                }
                var target = self.newConfiguration();
                generic.merge(self.oldConfiguration(), data.configuration, target, self.configurableStorageDriverAttrs);
                self.newConfiguration(target);
                self.oldConfiguration($.extend({}, data.configuration));
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
                self.iops(stats['4k_operations_ps']);
                self.cacheHits(stats.cache_hits_ps);
                self.cacheMisses(stats.cache_misses_ps);
                self.readSpeed(stats.data_read_ps);
                self.writeSpeed(stats.data_written_ps);
                self.backendWritten(stats.backend_data_written);
                self.backendRead(stats.backend_data_read);
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
