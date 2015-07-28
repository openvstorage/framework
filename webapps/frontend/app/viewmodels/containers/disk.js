// Copyright 2015 Open vStorage NV
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
    'viewmodels/containers/diskpartition'
], function($, ko, generic, api, Partition) {
    "use strict";
    return function(guid) {
        var self = this;

        // Handles
        self.loadHandle     = undefined;
        self.loadPartitions = undefined;

        // External dependencies
        self.storageRouter = ko.observable();

        // Observables
        self.trigger           = ko.observable();
        self.loading           = ko.observable(false);
        self.loaded            = ko.observable(false);
        self.guid              = ko.observable(guid);
        self.path              = ko.observable();
        self.vendor            = ko.observable();
        self.diskModel         = ko.observable();
        self.state             = ko.observable();
        self.name              = ko.observable();
        self.size              = ko.observable();
        self.isSsd             = ko.observable();
        self.storageRouterGuid = ko.observable();
        self.partitionsLoaded  = ko.observable(false);
        self.partitions        = ko.observableArray([]);

        // Computed
        self.formattedSize = ko.computed(function() {
            return generic.formatBytes(self.size());
        });
        self.enhancedPartitions = ko.computed(function() {
            var data = [], size = 0, previousOffset = 0, hasMountpoint = false;
            $.each(self.partitions(), function(index, partition) {
                if (partition.loaded() === true) {
                    data.push(partition);
                    if (partition.mountpoint() !== null) {
                        size += partition.size();
                    }
                }
            });
            $.each(data, function(index, partition) {
                if (partition.mountpoint() !== null) {
                    hasMountpoint = true;
                    partition.filledOffset = previousOffset;
                    partition.filledSize = self.size() / size * partition.size();
                    previousOffset += partition.filledSize;
                }
            });
            return data;
        }).extend({ rateLimit: { method: 'notifyWhenChangesStop', timeout: 100 } });
        self.emptySpaces = ko.computed(function() {
            var data = [], previousPartition, partition;
            if (self.partitions().length > 0) {
                $.each(self.partitions(), function (index, partition) {
                    if (previousPartition === undefined) {
                        if (partition.offset() !== 0) {
                            data.push({
                                offset: 0,
                                size: partition.offset()
                            });
                        }
                    } else if (previousPartition.offset() + previousPartition.size() < partition.offset()) {
                        data.push({
                            offset: previousPartition.offset() + previousPartition.size(),
                            size: partition.offset() - previousPartition.offset() - previousPartition.size()
                        });
                    }
                    previousPartition = partition;
                });
                partition = self.partitions()[self.partitions().length - 1];
                if (partition.offset() + partition.size() < self.size()) {
                    data.push({
                        offset: partition.offset() + partition.size(),
                        size: self.size() - partition.offset() - partition.size()
                    });
                }
            } else {
                data.push({
                    offset: 0,
                    size: self.size()
                });
            }
            return data;
        }).extend({ rateLimit: { method: 'notifyWhenChangesStop', timeout: 100 } });
        //

        // Functions
        self.fillData = function(data) {
            generic.trySet(self.path, data, 'path');
            generic.trySet(self.vendor, data, 'vendor');
            generic.trySet(self.diskModel, data, 'model');
            generic.trySet(self.state, data, 'state');
            generic.trySet(self.name, data, 'name');
            generic.trySet(self.size, data, 'size');
            generic.trySet(self.isSsd, data, 'is_ssd');
            generic.trySet(self.storageRouterGuid, data, 'storagerouter_guid');

            self.loaded(true);
            self.loading(false);
            self.trigger(generic.getTimestamp());
        };
        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                if (generic.xhrCompleted(self.loadHandle)) {
                    self.loadHandle = api.get('disks/' + self.guid())
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
        self.getPartitions = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadPartitions)) {
                    self.loadPartitions = api.get('diskpartitions', { queryparams: {
                        diskguid: self.guid(),
                        contents: '_relations',
                        sort: 'offset'
                    }})
                        .done(function(data) {
                            var guids = [], pdata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                pdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.partitions,
                                function(guid) {
                                    var p = new Partition(guid);
                                    p.loading(true);
                                    return p;
                                }, 'guid'
                            );
                            $.each(self.partitions(), function(index, partition) {
                                if (pdata.hasOwnProperty(partition.guid())) {
                                    partition.fillData(pdata[partition.guid()]);
                                }
                            });
                            self.partitionsLoaded(true);
                            self.trigger(generic.getTimestamp());
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
    };
});
