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
    'jquery', 'knockout', 'ovs/generic', './data'
], function($, ko, generic, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data            = data;
        self.mountpointRegex = /^(\/[a-zA-Z0-9\-_ \.]+)+\/?$/;

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, reasons = [], fields = [];
            if (self.data.backend() !== 'local' && self.data.backend() !== 'distributed') {
                self.data.mtptBFS('/'); // BFS isn't used, so it set to a non-conflicting value
            }
            if ((self.data.mtptFOC() === '/' || self.data.mtptReadCaches().contains(self.data.mtptFOC())) && $.inArray('foc', fields) === -1) {
                valid = false;
                fields.push('foc');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.cachefs') }));
            }
            if ((self.data.mtptBFS() === '/' || self.data.mtptReadCaches().contains(self.data.mtptBFS())) && $.inArray('bfs', fields) === -1 && (self.data.backend() === 'local' || self.data.backend() === 'distributed')) {
                valid = false;
                fields.push('bfs');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.bfs') }));
            }
            if (self.data.mtptMD() === '/' || self.data.mtptReadCaches().contains(self.data.mtptMD()) && $.inArray('md', fields) === -1) {
                valid = false;
                fields.push('md');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.mdfs') }));
            }
            if (self.data.mtptTemp() === '/' || self.data.mtptReadCaches().contains(self.data.mtptTemp()) && $.inArray('temp', fields) === -1) {
                valid = false;
                fields.push('temp');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.tempfs') }));
            }
            $.each(self.data.mtptReadCaches(), function(index, mp) {
                if (self.data.mtptBFS() === mp || self.data.mtptFOC() === mp || self.data.mtptMD() === mp || self.data.mtptTemp() === mp) {
                    if (!fields.contains('readcache')) {
                        fields.push('readcache')
                    }
                }
            });
            $.each(self.data.mtptWriteCaches(), function(index, mp) {
                if (self.data.mtptReadCaches().contains(mp) && mp !== '/') {
                    valid = false;
                    if (!fields.contains('readcache')) {
                        fields.push('readcache')
                    }
                    if (!fields.contains('writecache')) {
                        fields.push('writecache')
                    }
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.cachefs') }));
                }
            });
            if (self.data.mtptWriteCaches().contains('/') && !fields.contains('writecache')) {
                valid = false;
                fields.push('writecache');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.cachefs') }));
            }
            // mountpoints should not be used by previously configured storagedrivers/vpools
            $.each(self.data.storageDrivers(), function(index, storageDriver) {
                if (self.data.target() !== undefined && storageDriver.storageDriverID() === (self.data.name() + self.data.target().machineId())) {
                    return true;
                }
                var storagedriver_mountpoints = [];
                if (storageDriver.mountpointBFS() !== undefined) {
                    storagedriver_mountpoints.push(storageDriver.mountpointBFS());
                }
                if (storageDriver.mountpointMD() !== undefined) {
                    storagedriver_mountpoints.push(storageDriver.mountpointMD());
                }
                if (storageDriver.mountpointFOC() !== undefined) {
                    storagedriver_mountpoints.push(storageDriver.mountpointFOC());
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
                if (storagedriver_mountpoints.contains(self.data.mtptBFS()) && !fields.contains('bfs') && (self.data.backend() === 'local' || self.data.backend() === 'distributed')) {
                    valid = false;
                    fields.push('bfs');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', {what: $.t('ovs:generic.bfs')}));
                }
                if (storagedriver_mountpoints.contains(self.data.mtptMD()) && !fields.contains('md')) {
                    valid = false;
                    fields.push('md');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', {what: $.t('ovs:generic.mdfs')}));
                }
                if (storagedriver_mountpoints.contains(self.data.mtptTemp()) && !fields.contains('temp')) {
                    valid = false;
                    fields.push('temp');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', {what: $.t('ovs:generic.tempfs')}));
                }
                if (storagedriver_mountpoints.contains(self.data.mtptFOC()) && !fields.contains('foc')) {
                    valid = false;
                    fields.push('foc');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', {what: $.t('ovs:generic.cachefs')}));
                }
                $.each(self.data.mtptReadCaches(), function (i, e) {
                    if (storagedriver_mountpoints.contains(e) && !fields.contains('readcache')) {
                        valid = false;
                        fields.push('readcache');
                        reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', {what: $.t('ovs:generic.cachefs')}));
                    }
                });
                $.each(self.data.mtptWriteCaches(), function (i, e) {
                    if (storagedriver_mountpoints.contains(e) && !fields.contains('writecache')) {
                        valid = false;
                        fields.push('writecache');
                        reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', {what: $.t('ovs:generic.cachefs')}));
                    }
                });
                return true;
            });
            if ((self.data.mtptBFS() === self.data.mtptMD() ||
                 self.data.mtptReadCaches().contains(self.data.mtptBFS()) ||
                 self.data.mtptWriteCaches().contains(self.data.mtptBFS()) ||
                 self.data.mtptBFS() === self.data.mtptTemp()) && !fields.contains('bfs') &&
                 (self.data.backend() === 'local' || self.data.backend() === 'distributed')) {
                valid = false;
                fields.push('bfs');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.bfsnotshared'));
            }
            if (!self.data.allowVPool() && !fields.contains('vpool')) {
                valid = false;
                fields.push('vpool');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.vpoolnotallowed'));
            }

            // verify if mountpoints are valid
            $.each(self.data.mtptReadCaches(), function(index, cache) {
                if (!fields.contains('readcache') && cache.match(self.mountpointRegex) === null) {
                    valid = false;
                    fields.push('readcache');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.invalidmtpt', { what: $.t('ovs:generic.cachefs') }));
                }
            });
            $.each(self.data.mtptWriteCaches(), function (index, cache) {
                if (!fields.contains('writecache') && cache.match(self.mountpointRegex) === null) {
                    valid = false;
                    fields.push('writecache');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.invalidmtpt', {what: $.t('ovs:generic.cachefs')}));
                }
            });
            if (!self.data.mtptFOC.valid()) {
                valid = false;
                fields.push('foc');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.invalidmtpt', { what: $.t('ovs:generic.cachefs') }));
            }
            if (!self.data.mtptTemp.valid()) {
                valid = false;
                fields.push('temp');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.invalidmtpt', { what: $.t('ovs:generic.tempfs') }));
            }
            if (!self.data.mtptBFS.valid() && (self.data.backend() === 'local' || self.data.backend() === 'distributed')) {
                valid = false;
                fields.push('bfs');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.invalidmtpt', { what: $.t('ovs:generic.bfs') }));
            }
            if (!self.data.mtptMD.valid()) {
                valid = false;
                fields.push('md');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.invalidmtpt', { what: $.t('ovs:generic.mdfs') }));
            }
            return { value: valid, reasons: reasons, fields: fields };
        });

        self.addReadCache = function() {
            var value = self.data.mtptCustomRC();
            if (value !== undefined && value !== '') {
                if ($.inArray(value, self.data.mtptCustomRCs()) === -1 && $.inArray(value, self.data.mountpoints()) === -1) {
                    self.data.mtptCustomRCs.push(value);
                    self.data.mtptReadCaches.push(value);
                }
                self.data.mtptCustomRC('');
            }
        };

        self.addWriteCache = function() {
            var value = self.data.mtptCustomWC();
            if (value !== undefined && value !== '') {
                if ($.inArray(value, self.data.mtptCustomWCs()) === -1 && $.inArray(value, self.data.mountpoints()) === -1) {
                    self.data.mtptCustomWCs.push(value);
                    self.data.mtptWriteCaches.push(value);
                }
                self.data.mtptCustomWC('');
            }
        };

        self.activate = function() {
            if (self.data.readcaches().length >= 1) {
                if ($.inArray(self.data.readcaches()[0], self.data.mtptReadCaches()) === -1) {
                    self.data.mtptReadCaches.push(self.data.readcaches()[0]);
                }
            }
            if (self.data.writecaches().length >= 1) {
                if ($.inArray(self.data.writecaches()[0], self.data.mtptWriteCaches()) === -1) {
                    self.data.mtptWriteCaches.push(self.data.writecaches()[0]);
                }
            }
        }
    };
});
