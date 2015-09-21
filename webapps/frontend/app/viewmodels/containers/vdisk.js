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
    'ovs/generic', 'ovs/api', 'ovs/shared'
], function($, ko, generic, api, shared) {
    "use strict";
    return function(guid) {
        var self = this;

        // Variables
        self.shared = shared;

        // Handles
        self.loadHandle       = undefined;
        self.loadParentConfig = undefined;

        // External dependencies
        self.storageRouter = ko.observable();
        self.vMachine      = ko.observable();
        self.vpool         = ko.observable();

        // Observables
        self.loading             = ko.observable(false);
        self.loaded              = ko.observable(false);
        self.guid                = ko.observable(guid);
        self.name                = ko.observable();
        self.order               = ko.observable(0);
        self.snapshots           = ko.observableArray([]);
        self.configuration       = ko.observable({});
        self.parentConfiguration = ko.observable();
        self.oldConfiguration    = ko.observable();
        self.size                = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.storedData          = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.cacheHits           = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.cacheMisses         = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.iops                = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.readSpeed           = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.writeSpeed          = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.backendWritten      = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.backendRead         = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.bandwidthSaved      = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.namespace           = ko.observable();
        self.storageRouterGuid   = ko.observable();
        self.vpoolGuid           = ko.observable();
        self.vMachineGuid        = ko.observable();
        self.failoverMode        = ko.observable();
        self.cacheStrategies     = ko.observableArray(['onread', 'onwrite', 'none']);
        self.dtlModes            = ko.observableArray(['nosync', 'async', 'sync']);
        self.dedupeModes         = ko.observableArray(['dedupe', 'nondedupe']);
        self.scoSizes            = ko.observableArray([4, 8, 16, 32, 64, 128]);

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
        self.cacheStrategy = ko.computed({
            read: function() {
                if (self.configuration() !== undefined && self.configuration().hasOwnProperty('cache_strategy')) {
                    return self.configuration().cache_strategy;
                }
                return undefined;
            },
            write: function(value) {
                var target = self.configuration();
                target.cache_strategy = value;
                self.configuration(target);
            }
        });
        self.dedupeMode = ko.computed({
            read: function() {
                if (self.configuration() !== undefined && self.configuration().hasOwnProperty('dedupe_mode')) {
                    return self.configuration().dedupe_mode;
                }
                return undefined;
            },
            write: function(value) {
                var target = self.configuration();
                target.dedupe_mode = value;
                self.configuration(target);
            }
        });
        self.dtlEnable = ko.computed({
            read: function() {
                if (self.configuration() !== undefined && self.configuration().hasOwnProperty('dtl_enabled')) {
                    return self.configuration().dtl_enabled;
                }
                return undefined;
            },
            write: function(value) {
                var target = self.configuration();
                target.dtl_enabled = value;
                self.configuration(target);
            }
        });
        self.dtlMode = ko.computed({
            read: function() {
                if (self.configuration() !== undefined && self.configuration().hasOwnProperty('dtl_mode')) {
                    return self.configuration().dtl_mode;
                }
                return undefined;
            },
            write: function(value) {
                var target = self.configuration();
                target.dtl_mode = value;
                self.configuration(target);
            }
        });
        self.scoSize = ko.computed({
            read: function() {
                if (self.configuration() !== undefined && self.configuration().hasOwnProperty('sco_size')) {
                    return self.configuration().sco_size;
                }
                return undefined;
            },
            write: function(value) {
                var target = self.configuration();
                target.sco_size = value;
                self.configuration(target);
            }
        });
        self.writeBuffer = ko.computed({
            read: function() {
                if (self.configuration() !== undefined && self.configuration().hasOwnProperty('write_buffer')) {
                    return self.configuration().write_buffer;
                }
                return undefined;
            },
            write: function(value) {
                var target = self.configuration();
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
                self.configuration(target);
            }
        }).extend({ notify: 'always' });
        self.configChanged = ko.computed(function() {
            var changed = false;
            if (self.configuration() !== undefined && self.oldConfiguration() !== undefined) {
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
                self.failoverMode(data.info.failover_mode.toLowerCase() || 'unknown');
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
                        if (data.write_buffer !== undefined) {
                            data.write_buffer = Math.round(data.write_buffer);
                        }
                        if (self.parentConfiguration() === undefined) {
                            self.parentConfiguration(data);
                        }
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };
        self.loadConfiguration = function() {
            return $.Deferred(function(deferred) {
                self.loadParentConfig = api.get('vpools/' + self.vpoolGuid() + '/get_configuration')
                    .then(self.shared.tasks.wait)
                    .done(function(data) {
                        if (data.write_buffer !== undefined) {
                            data.write_buffer = Math.round(data.write_buffer);
                        }
                        self.configuration(data);
                        if (self.oldConfiguration() === undefined) {
                            self.oldConfiguration($.extend({}, data));  // Used to make comparison to check for changes
                        }
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };
        self.loadAllConfigurations = function() {
            self.loadConfiguration();
            self.loadParentConfiguration();
        };
    };
});
