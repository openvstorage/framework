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
    'ovs/generic', 'ovs/api',
    'viewmodels/containers/backendtype', 'viewmodels/containers/vdisk', 'viewmodels/containers/vmachine'
], function($, ko, generic, api, BackendType, VDisk, VMachine) {
    "use strict";
    return function(guid) {
        var self = this;

        // Variables
        self.configurableStorageDriverAttrs = ['cache_strategy', 'dedupe_mode', 'dtl_mode', 'sco_size', 'write_buffer'];

        // Handles
        self.loadHandle          = undefined;
        self.diskHandle          = undefined;
        self.machineHandle       = undefined;
        self.storageRouterHandle = undefined;

        // Observables
        self.loading            = ko.observable(false);
        self.loaded             = ko.observable(false);
        self.guid               = ko.observable(guid);
        self.name               = ko.observable();
        self.oldConfiguration   = ko.observable();
        self.newConfiguration   = ko.observable();
        self.size               = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.iops               = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.storedData         = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.cacheHits          = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.cacheMisses        = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.readSpeed          = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.writeSpeed         = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.backendWriteSpeed  = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.backendReadSpeed   = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.backendWritten     = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.backendRead        = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.bandwidthSaved     = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.backendTypeGuid    = ko.observable();
        self.backendType        = ko.observable();
        self.backendConnection  = ko.observable();
        self.backendLogin       = ko.observable();
        self.vDisks             = ko.observableArray([]);
        self.vMachines          = ko.observableArray([]);
        self.storageRouterGuids = ko.observableArray([]);
        self.storageDriverGuids = ko.observableArray([]);
        self.cacheStrategies    = ko.observableArray([undefined, { name: 'onread' }, { name: 'onwrite' }, { name: 'none' }]);
        self.dtlModes           = ko.observableArray([undefined, { name: 'nosync' }, { name: 'async' }, { name: 'sync' }]);
        self.dedupeModes        = ko.observableArray([undefined, { name: 'dedupe' }, { name: 'nondedupe' }]);
        self.scoSizes           = ko.observableArray([undefined, 4, 8, 16, 32, 64, 128]);

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
        self.fillData = function(data, options) {
            options = options || {};
            generic.trySet(self.name, data, 'name');
            generic.trySet(self.storedData, data, 'stored_data');
            generic.trySet(self.size, data, 'size');
            generic.trySet(self.backendConnection, data, 'connection');
            generic.trySet(self.backendLogin, data, 'login');
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
            if (data.hasOwnProperty('backend_type_guid')) {
                self.backendTypeGuid(data.backend_type_guid);
            } else {
                self.backendTypeGuid(undefined);
            }
            if (data.hasOwnProperty('vdisks_guids') && !generic.tryGet(options, 'skipDisks', false)) {
                generic.crossFiller(
                    data.vdisks_guids, self.vDisks,
                    function(guid) {
                        return new VDisk(guid);
                    }, 'guid'
                );
            }
            if (data.hasOwnProperty('storagedrivers_guids')) {
                self.storageDriverGuids(data.storagedrivers_guids);
            }
            if (data.hasOwnProperty('statistics')) {
                var stats = data.statistics;
                self.iops(stats.operations_ps);
                self.cacheHits(stats.cache_hits_ps);
                self.cacheMisses(stats.cache_misses_ps);
                self.readSpeed(stats.data_read_ps);
                self.writeSpeed(stats.data_written_ps);
                self.backendWritten(stats.backend_data_written);
                self.backendRead(stats.backend_data_read);
                self.bandwidthSaved(Math.max(0, stats.data_read - stats.backend_data_read));
                self.backendReadSpeed(stats.backend_data_read_ps);
                self.backendWriteSpeed(stats.backend_data_written_ps);
            }

            self.loaded(true);
            self.loading(false);
        };
        self.load = function(contents, options) {
            options = options || {};
            self.loading(true);
            return $.Deferred(function(deferred) {
                var calls = [
                    $.Deferred(function(mainDeferred) {
                        if (generic.xhrCompleted(self.loadHandle)) {
                            var listOptions = {};
                            if (contents !== undefined) {
                                listOptions.contents = contents;
                            }
                            self.loadHandle = api.get('vpools/' + self.guid(), { queryparams: listOptions })
                                .done(function(data) {
                                    self.fillData(data, options);
                                    mainDeferred.resolve();
                                })
                                .fail(mainDeferred.reject);
                        } else {
                            mainDeferred.resolve();
                        }
                    }).promise(),
                    $.Deferred(function(machineDeferred) {
                        if (generic.xhrCompleted(self.machineHandle)) {
                            var options = {
                                sort: 'name',
                                vpoolguid: self.guid(),
                                contents: ''
                            };
                            self.machineHandle = api.get('vmachines', { queryparams: options })
                                .done(function(data) {
                                    var guids = [], vmdata = {};
                                    $.each(data.data, function(index, item) {
                                        guids.push(item.guid);
                                        vmdata[item.guid] = item;
                                    });
                                    generic.crossFiller(
                                        guids, self.vMachines,
                                        function(guid) {
                                            var vmachine = new VMachine(guid);
                                            if ($.inArray(guid, guids) !== -1) {
                                                vmachine.fillData(vmdata[guid]);
                                            }
                                            vmachine.loading(true);
                                            return vmachine;
                                        }, 'guid'
                                    );
                                    machineDeferred.resolve();
                                })
                                .fail(machineDeferred.reject);
                        } else {
                            machineDeferred.resolve();
                        }
                    }).promise()];
                $.when.apply($, calls)
                    .done(function() {
                        self.loaded(true);
                        deferred.resolve();
                    })
                    .fail(deferred.reject)
                    .always(function() {
                        self.loading(false);
                    });
            }).promise();
        };
        self.loadStorageRouters = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.storageRouterHandle)) {
                    self.storageRouterHandle = api.get('vpools/' + self.guid() + '/storagerouters')
                        .done(function(data) {
                            self.storageRouterGuids(data.data);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.loadBackendType = function(refresh) {
            refresh = !!refresh;
            return $.Deferred(function(deferred) {
                if (self.backendTypeGuid() !== undefined) {
                    if (self.backendType() === undefined || self.backendTypeGuid() !== self.backendType().guid()) {
                        var backendType = new BackendType(self.backendTypeGuid());
                        backendType.load()
                            .then(deferred.resolve)
                            .fail(deferred.reject);
                        self.backendType(backendType);
                    } else if (refresh) {
                        self.backendType().load()
                            .then(deferred.resolve)
                            .fail(deferred.reject);
                    }
                } else {
                    self.backendType(undefined);
                    deferred.resolve();
                }
            }).promise();
        };
    };
});
