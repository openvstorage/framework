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
    'jquery', 'knockout',
    'ovs/generic', 'ovs/api', 'ovs/shared'
], function($, ko, generic, api, shared) {
    "use strict";
    return function(guid) {
        var self = this;

        // Variables
        self.shared = shared;

        // Handles
        self.loadHandle       = undefined;
        self.loadConfig       = undefined;
        self.loadParentConfig = undefined;

        // External dependencies
        self.dtlTargets    = ko.observableArray([]);
        self.storageRouter = ko.observable();
        self.vMachine      = ko.observable();
        self.vpool         = ko.observable();

        // Observables
        self.backendRead         = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.backendWritten      = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.bandwidthSaved      = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.cacheHits           = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.cacheMisses         = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.cacheStrategies     = ko.observableArray(['on_read', 'on_write', 'none']);
        self.cacheStrategy       = ko.observable('on_read');
        self.dedupeMode          = ko.observable();
        self.dedupeModes         = ko.observableArray([{name: 'dedupe', disabled: false}, {name: 'non_dedupe', disabled: false}]);
        self.dtlEnabled          = ko.observable(true);
        self.dtlMode             = ko.observable();
        self.dtlModes            = ko.observableArray([{name: 'no_sync', disabled: false}, {name: 'a_sync', disabled: false}, {name: 'sync', disabled: false}]);
        self.dtlStatus           = ko.observable();
        self.dtlTarget           = ko.observable();
        self.guid                = ko.observable(guid);
        self.iops                = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.loaded              = ko.observable(false);
        self.loading             = ko.observable(false);
        self.loadingConfig       = ko.observable(false);
        self.name                = ko.observable();
        self.namespace           = ko.observable();
        self.oldConfiguration    = ko.observable();
        self.order               = ko.observable(0);
        self.parentConfiguration = ko.observable();
        self.readCacheLimit      = ko.observable().extend({numeric: {min: 1, max: 10240, allowUndefined: true}});
        self.readSpeed           = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.scoSize             = ko.observable(4);
        self.scoSizes            = ko.observableArray([4, 8, 16, 32, 64, 128]);
        self.size                = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.snapshots           = ko.observableArray([]);
        self.storageRouterGuid   = ko.observable();
        self.storedData          = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.vMachineGuid        = ko.observable();
        self.vpoolGuid           = ko.observable();
        self.writeSpeed          = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.writeBuffer         = ko.observable(128).extend({numeric: {min: 128, max: 10240}});

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
        self.dtlModeChange = ko.computed({
            read: function() {
                if (self.dtlTargets().length === 0) {
                    self.dtlMode('no_sync');
                    return {name: self.dtlMode(), disabled: true};
                }
                return {name: self.dtlMode(), disabled: false};
            },
            write: function(mode) {
                if (mode === 'no_sync') {
                    self.dtlEnabled(false);
                } else {
                    self.dtlEnabled(true);
                    if (self.dtlTargets().length === 0) {
                        self.dtlTarget(null);
                        $.each(self.dtlModes(), function(index, item) {
                            item.disabled = true;
                        })
                    } else if (self.dtlTarget() === null) {
                        self.dtlTarget(self.dtlTargets()[0]);
                    }
                }
                self.dtlMode(mode.name);
            }
        });
        self.configuration = ko.computed({
            read: function() {
                return {sco_size: self.scoSize(),
                        dtl_mode: self.dtlMode(),
                        dtl_enabled: self.dtlEnabled(),
                        dedupe_mode: self.dedupeMode() !== undefined ? self.dedupeMode().name : undefined,
                        write_buffer: self.writeBuffer(),
                        dtl_target: self.dtlTarget() || null,
                        cache_strategy: self.cacheStrategy(),
                        readcache_limit: self.readCacheLimit() || null}
            },
            write: function(configData) {
                self.writeBuffer(Math.round(configData.write_buffer));
                self.scoSize(configData.sco_size);
                self.dtlMode(configData.dtl_mode);
                self.dedupeMode({name: configData.dedupe_mode, disabled: false});
                self.dtlTarget(configData.dtl_target === null ? null : configData.dtl_target);
                self.cacheStrategy(configData.cache_strategy);
                self.readCacheLimit(configData.readcache_limit);
            }
        });
        self.scoSize.subscribe(function(size) {
            if (size < 128) {
                self.writeBuffer.min = 128;
            } else {
                self.writeBuffer.min = 256;
            }
            self.writeBuffer(self.writeBuffer());
        });
        self.configChanged = ko.computed(function() {
            var changed = false;
            if (self.oldConfiguration() !== undefined) {
                $.each(self.oldConfiguration(), function (key, i) {
                    if (!self.configuration().hasOwnProperty(key)) {
                        changed = true;
                        return false;
                    }
                    if (self.configuration()[key] !== self.oldConfiguration()[key]) {
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
            if (data.hasOwnProperty('info')) {
                self.storedData(data.info.stored);
                self.dtlStatus(data.info.failover_mode.toLowerCase() || 'unknown');
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
        self.loadParentConfiguration = function() {
            return $.Deferred(function(deferred) {
                self.loadParentConfig = api.get('vpools/' + self.vpoolGuid() + '/get_configuration')
                    .then(self.shared.tasks.wait)
                    .done(function(data) {
                        if (self.parentConfiguration() === undefined) {
                            self.parentConfiguration(data);
                        }
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };
        self.loadConfiguration = function(reload) {
            self.loadingConfig(true);
            return $.Deferred(function(deferred) {
                self.loadConfig = api.get('vdisks/' + self.guid() + '/get_config_params')
                    .then(self.shared.tasks.wait)
                    .done(function(data) {
                        if (data.dedupe_mode !== undefined && data.dedupe_mode === 'non_dedupe') {
                            $.each(self.dedupeModes(), function (i, key) {
                                if (key.name === 'dedupe') {
                                    self.dedupeModes()[i].disabled = true;
                                    return false;
                                }
                            });
                        }
                        self.configuration(data);
                        if (self.oldConfiguration() === undefined || reload === true) {
                            self.oldConfiguration($.extend({}, data));  // Used to make comparison to check for changes
                            $.each(self.oldConfiguration(), function (key, value) {
                                var oldConfig;
                                if (key === 'write_buffer') {
                                    oldConfig = self.oldConfiguration();
                                    oldConfig.write_buffer = Math.round(self.oldConfiguration().write_buffer);
                                    self.oldConfiguration(oldConfig);
                                } else if (key === 'dtl_target' && value === null) {
                                    oldConfig = self.oldConfiguration();
                                    oldConfig.dtl_target = null;
                                    self.oldConfiguration(oldConfig);
                                }
                            });
                        }
                        deferred.resolve();
                    })
                    .fail(deferred.reject)
                    .always(function() {
                        self.loadingConfig(false);
                    });

            }).promise();
        };
    };
});
