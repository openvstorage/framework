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
    'jquery', 'knockout', 'ovs/generic', './data', 'ovs/api', '../../containers/storagerouter',
    '../../containers/storagedriver', 'ovs/shared'
], function($, ko, generic, data, api, StorageRouter, StorageDriver, shared) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data = data;
        self.shared = shared;

        // Computed
        self.canContinue = ko.computed(function() {
            var reasons = [], fields = [], storagedriver_mountpoints = [], bfs_mountpoints = [];

            // Collect previously configured storagedriver mountpoints
            $.each(self.data.storageDrivers(), function(index, storageDriver) {
                if (self.data.target() !== undefined && storageDriver.storageDriverID() === (self.data.name() + self.data.target().machineId())) {
                    return true;
                }
                if (storageDriver.mountpointBFS() !== undefined && storageDriver.mountpointBFS() !== "") {
                    bfs_mountpoints.push(storageDriver.mountpointBFS());
                    storagedriver_mountpoints.push(storageDriver.mountpointBFS());
                }
                if (storageDriver.mountpointMD() !== undefined) {
                    storagedriver_mountpoints.push(storageDriver.mountpointMD());
                }
                if (storageDriver.mountpointDTL() !== undefined) {
                    storagedriver_mountpoints.push(storageDriver.mountpointDTL());
                }
                if (storageDriver.mountpointTemp() !== undefined) {
                    storagedriver_mountpoints.push(storageDriver.mountpointTemp());
                }
                if (storageDriver.mountpointReadCaches() !== undefined) {
                    $.each(storageDriver.mountpointReadCaches(), function (i, e) {
                        storagedriver_mountpoints.push(e.substring(0, e.lastIndexOf('/')));
                    });
                }
                if (storageDriver.mountpointWriteCaches() !== undefined) {
                    $.each(storageDriver.mountpointWriteCaches(), function (i, e) {
                        storagedriver_mountpoints.push(e.substring(0, e.lastIndexOf('/')));
                    });
                }
                return true;
            });

            // BFS mountpoint checks
            if (self.data.backend() === 'local' || self.data.backend() === 'distributed') {
                if (self.data.mtptBFS() !== '' && storagedriver_mountpoints.contains(self.data.mtptBFS())) {
                    fields.push('bfs');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', {what: $.t('ovs:wizards.addvpool.gathermountpoints.bfsname')}));
                }
                else {
                    $.each(bfs_mountpoints, function (index, mp) {
                        if (self.data.mtptBFS() !== undefined && self.data.mtptBFS().startsWith(mp + '/')) {
                            fields.push('bfs');
                            reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.bfsnotshared'));
                        }
                    });
                }
                if (self.data.mtptBFS() !== '' &&
                   (self.data.mtptBFS() === self.data.mtptMD() ||
                    self.data.mtptBFS() === self.data.mtptTemp() ||
                    self.data.mtptReadCaches().contains(self.data.mtptBFS()) ||
                    self.data.mtptWriteCaches().contains(self.data.mtptBFS()))) {
                        fields.push('bfs');
                        reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.uniquemtpt', {what: $.t('ovs:wizards.addvpool.gathermountpoints.bfsname')}));
                }
                if (!self.data.mtptBFS.valid()) {
                    fields.push('bfs');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.invalidmtpt', { what: $.t('ovs:wizards.addvpool.gathermountpoints.bfsname') }));
                }
            }
            else {
                self.data.mtptBFS('');
            }

            // Temp mountpoint checks
            if (self.data.mtptTemp() !== '' && storagedriver_mountpoints.contains(self.data.mtptTemp())) {
                fields.push('temp');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', {what: $.t('ovs:wizards.addvpool.gathermountpoints.tempname')}));
            }
            if (!self.data.mtptTemp.valid()) {
                fields.push('temp');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.invalidmtpt', { what: $.t('ovs:wizards.addvpool.gathermountpoints.tempname') }));
            }

            // MD mountpoint checks
            if (self.data.mtptMD() !== '' && storagedriver_mountpoints.contains(self.data.mtptMD())) {
                fields.push('md');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', {what: $.t('ovs:wizards.addvpool.gathermountpoints.mdname')}));
            }
            if (!self.data.mtptMD.valid()) {
                fields.push('md');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.invalidmtpt', { what: $.t('ovs:wizards.addvpool.gathermountpoints.mdname') }));
            }

            // Read cache mountpoint checks
            if (self.data.mtptReadCaches().length === 0) {
                fields.push('readcache');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.atleastone', {what: $.t('ovs:wizards.addvpool.gathermountpoints.readcachename')}));
            }
            $.each(self.data.mtptReadCaches(), function (index, mp) {
                if (storagedriver_mountpoints.contains(mp)) {
                    fields.push('readcache');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', {what: $.t('ovs:wizards.addvpool.gathermountpoints.readcachename')}));
                }
                if (self.data.mtptBFS() === mp || self.data.mtptDTL() === mp || self.data.mtptMD() === mp || self.data.mtptTemp() === mp || self.data.mtptWriteCaches().contains(mp)) {
                    fields.push('readcache');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.uniquemtpt', {what: $.t('ovs:wizards.addvpool.gathermountpoints.readcachename')}));
                }
                if (mp.match(self.data.mountpointRegex) === null) {
                    fields.push('readcache');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.invalidmtpt', { what: $.t('ovs:wizards.addvpool.gathermountpoints.readcachename') }));
                }
            });

            // Write cache mountpoint checks
            if (self.data.mtptWriteCaches().length === 0) {
                fields.push('writecache');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.atleastone', {what: $.t('ovs:wizards.addvpool.gathermountpoints.writecachename')}));
            }
            $.each(self.data.mtptWriteCaches(), function (index, mp) {
                if (storagedriver_mountpoints.contains(mp)) {
                    fields.push('writecache');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', {what: $.t('ovs:wizards.addvpool.gathermountpoints.writecachename')}));
                }
                if (mp.match(self.data.mountpointRegex) === null) {
                    fields.push('writecache');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.invalidmtpt', {what: $.t('ovs:wizards.addvpool.gathermountpoints.writecachename')}));
                }
            });

            // DTL mountpoint checks
            if (self.data.mtptDTL() !== '' && storagedriver_mountpoints.contains(self.data.mtptDTL())) {
                fields.push('dtl');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', {what: $.t('ovs:wizards.addvpool.gathermountpoints.dtl_name')}));
            }
            if (!self.data.mtptDTL.valid()) {
                fields.push('dtl');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.invalidmtpt', { what: $.t('ovs:wizards.addvpool.gathermountpoints.dtl_name') }));
            }

            // vPool check
            if (!self.data.allowVPool()) {
                fields.push('vpool');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.vpoolnotallowed'));
            }
            var valid = reasons.length === 0;
            var unique_fields = fields.filter(generic.arrayFilterUnique);
            var unique_reasons = reasons.filter(generic.arrayFilterUnique);
            return { value: valid, reasons: unique_reasons, fields: unique_fields };
        });

        self.addReadCache = function() {
            var value = self.data.mtptCustomRC();
            if (value !== undefined && value !== '') {
                if (!self.data.mtptCustomRCs().contains(value)) {
                    self.data.mtptCustomRCs.push(value);
                }
                if (!self.data.mtptReadCaches().contains(value)) {
                    self.data.mtptReadCaches.push(value);
                }
                self.data.mtptCustomRC('');
            }
        };

        self.addWriteCache = function() {
            var value = self.data.mtptCustomWC();
            if (value !== undefined && value !== '') {
                if (!self.data.mtptCustomWCs().contains(value)) {
                    self.data.mtptCustomWCs.push(value);
                }
                if (!self.data.mtptWriteCaches().contains(value)) {
                    self.data.mtptWriteCaches.push(value);
                }
                self.data.mtptCustomWC('');
            }
        };

        self.activate = function() {
            if (data.extendVpool() === true) {
                self.loadStorageRoutersHandle = api.get('storagerouters', {
                        queryparams: {
                        contents: 'storagedrivers',
                        sort: 'name'
                    }
                }).done(function(data) {
                        var guids = [], srdata = {};
                        $.each(data.data, function(index, item) {
                            guids.push(item.guid);
                            srdata[item.guid] = item;
                        });
                        generic.crossFiller(
                            guids, self.data.storageRouters,
                            function(guid) {
                                return new StorageRouter(guid);
                            }, 'guid'
                        );
                        $.each(self.data.storageRouters(), function(index, storageRouter) {
                            storageRouter.fillData(srdata[storageRouter.guid()]);
                        });
                        if (self.data.target() === undefined && self.data.storageRouter() !== undefined) {
                            self.data.target(self.data.storageRouter);
                        }
                    });

                if (data.vPool() !== undefined) {
                    self.data.vPool().load('storagedrivers', { skipDisks: true })
                        .then(function() {
                            self.data.storageDriver(new StorageDriver(self.data.vPool().storageDriverGuids()[0]));
                            self.data.storageDriver().load()
                                .then(function() {
                                    self.data.storageIP(self.data.storageDriver().storageIP())
                                });
                            self.data.vPool().backendType().load();
                            self.data.backend(self.data.vPool().backendType().name());
                            self.data.name(self.data.vPool().name());
                            if (self.data.storageRouter() !== undefined) {
                                api.post('storagerouters/' + self.data.storageRouter().guid() + '/get_physical_metadata')
                                .then(self.shared.tasks.wait)
                                .then(function(data) {
                                        self.data.mountpoints(data.mountpoints);
                                        self.data.readcaches(data.readcaches);
                                        self.data.writecaches(data.writecaches);
                                        self.data.ipAddresses(data.ipaddresses);
                                        self.data.files(data.files);
                                        self.data.allowVPool(data.allow_vpool);
                                        if (self.data.mountpoints().length >= 1) {
                                            self.data.mtptBFS(self.data.mountpoints()[0]);
                                        }
                                        if (self.data.writecaches().length >= 1) {
                                            self.data.mtptMD(self.data.writecaches()[0]);
                                            self.data.mtptDTL(self.data.writecaches()[0]);
                                        }
                                        if (self.data.readcaches().length >= 1) {
                                            if (!self.data.mtptReadCaches().contains(self.data.readcaches()[0])) {
                                                self.data.mtptReadCaches.push(self.data.readcaches()[0]);
                                            }
                                        }
                                        if (self.data.writecaches().length >= 1) {
                                            if (!self.data.mtptWriteCaches().contains(self.data.writecaches()[0])) {
                                                self.data.mtptWriteCaches.push(self.data.writecaches()[0]);
                                            }
                                        }
                                })
                            }
                        })
                }
            }
            if (self.data.readcaches().length >= 1) {
                if (!self.data.mtptReadCaches().contains(self.data.readcaches()[0])) {
                    self.data.mtptReadCaches.push(self.data.readcaches()[0]);
                }
            }
            if (self.data.writecaches().length >= 1) {
                if (!self.data.mtptWriteCaches().contains(self.data.writecaches()[0])) {
                    self.data.mtptWriteCaches.push(self.data.writecaches()[0]);
                }
            }
        }
    };
});
