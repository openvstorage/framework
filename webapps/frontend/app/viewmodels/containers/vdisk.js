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
    'ovs/generic', 'ovs/api'
], function($, ko, generic, api) {
    "use strict";
    return function(guid) {
        var self = this;

        // Variables
        self.configurableStorageDriverAttrs = ['cache_strategy', 'dedupe_mode', 'dtl_enabled', 'dtl_mode', 'sco_size', 'write_buffer'];

        // Handles
        self.loadHandle = undefined;

        // External dependencies
        self.storageRouter = ko.observable();
        self.vMachine      = ko.observable();
        self.vpool         = ko.observable();

        // Observables
        self.loading           = ko.observable(false);
        self.loaded            = ko.observable(false);
        self.guid              = ko.observable(guid);
        self.name              = ko.observable();
        self.order             = ko.observable(0);
        self.snapshots         = ko.observableArray([]);
        self.oldConfiguration  = ko.observable();
        self.newConfiguration  = ko.observable();
        self.size              = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.storedData        = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.cacheHits         = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.cacheMisses       = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.iops              = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.readSpeed         = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.writeSpeed        = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.backendWritten    = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.backendRead       = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.bandwidthSaved    = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.namespace         = ko.observable();
        self.storageRouterGuid = ko.observable();
        self.vpoolGuid         = ko.observable();
        self.vMachineGuid      = ko.observable();
        self.dtlMode           = ko.observable();
        self.cacheStrategies   = ko.observableArray([undefined, { name: 'onread' }, { name: 'onwrite' }, { name: 'none' }]);
        self.dtlModes          = ko.observableArray([undefined, { name: 'nosync' }, { name: 'async' }, { name: 'sync' }]);
        self.dedupeModes       = ko.observableArray([undefined, { name: 'dedupe' }, { name: 'nondedupe' }]);
        self.dtlOptions        = ko.observableArray([undefined, { value: true }, { value: false }]);
        self.scoSizes          = ko.observableArray([undefined, 4, 8, 16, 32, 64, 128]);

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
        self.fallbackConfig = ko.computed(function() {
            var fallback = {};
            if (self.vMachine() !== undefined) {
                if (self.vMachine().configCacheStrategy() !== undefined) {
                    fallback.cache_strategy = [self.vMachine().configCacheStrategy().name, 'machine'];
                }
                if (self.vMachine().dedupeMode() !== undefined) {
                    fallback.dedupe_mode = [self.vMachine().dedupeMode().name, 'machine'];
                }
                if (self.vMachine().dtlEnable() !== undefined) {
                    fallback.dtl_enabled = [self.vMachine().dtlEnable().value, 'machine'];
                }
                if (self.vMachine().dtlMode() !== undefined) {
                    fallback.dtl_mode = [self.vMachine().dtlMode().name, 'machine'];
                }
                if (self.vMachine().scoSize() !== undefined) {
                    fallback.sco_size = [self.vMachine().scoSize(), 'machine'];
                }
                if (self.vMachine().writeBuffer() !== undefined) {
                    fallback.write_buffer = [self.vMachine().writeBuffer(), 'machine'];
                }
            }
            return fallback;
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
        self.fillData = function(data) {
            generic.trySet(self.name, data, 'name');
            generic.trySet(self.order, data, 'order');
            if (data.hasOwnProperty('snapshots')) {
                var snapshots = [];
                $.each(data.snapshots, function(index, snapshot) {
                    if (snapshot.in_backend) {
                        snapshots.push(snapshot);
                    }
                });
                self.snapshots(snapshots);
            }
            generic.trySet(self.size, data, 'size');
            generic.trySet(self.vpoolGuid, data, 'vpool_guid');
            generic.trySet(self.vMachineGuid, data, 'vmachine_guid');
            generic.trySet(self.storageRouterGuid, data, 'storagerouter_guid');
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
            if (data.hasOwnProperty('info')) {
                self.storedData(data.info.stored);
                self.dtlMode(data.info.dtl_mode.toLowerCase() || 'unknown');
                self.namespace(data.info.namespace);
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
                // Sorting based on newest first
                return b.timestamp - a.timestamp;
            });

            self.loaded(true);
            self.loading(false);
        };
        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                if (generic.xhrCompleted(self.loadHandle)) {
                    self.loadHandle = api.get('vdisks/' + self.guid())
                        .done(function(data) {
                            self.fillData(data);
                            deferred.resolve();
                        })
                        .fail(deferred.reject)
                        .always(function() {
                            self.loading(false);
                        });
                } else {
                    deferred.reject();
                }
            }).promise();
        };
    };
});
