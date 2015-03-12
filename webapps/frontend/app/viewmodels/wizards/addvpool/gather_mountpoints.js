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
    './data'
], function($, ko, data) {
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
            if (self.data.mtptFOC() === '/' && $.inArray('foc', fields) === -1) {
                valid = false;
                fields.push('foc');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.cachefs') }));
            }
            if (self.data.mtptBFS() === '/' && $.inArray('bfs', fields) === -1 && (self.data.backend() === 'local' || self.data.backend() === 'distributed')) {
                valid = false;
                fields.push('bfs');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.bfs') }));
            }
            if (self.data.mtptMD() === '/' && $.inArray('md', fields) === -1) {
                valid = false;
                fields.push('md');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.mdfs') }));
            }
            if (self.data.mtptTemp() === '/' && $.inArray('temp', fields) === -1) {
                valid = false;
                fields.push('temp');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.tempfs') }));
            }
            $.each(self.data.storageDrivers(), function(index, storageDriver) {
                if (self.data.target() !== undefined && storageDriver.storageDriverID() === (self.data.name() + self.data.target().machineId())) {
                    return true;
                }
                if ((self.data.mtptReadCache1() === storageDriver.mountpointReadCache1() ||
                     self.data.mtptReadCache1() === storageDriver.mountpointReadCache2() ||
                     self.data.mtptReadCache1() === storageDriver.mountpointWriteCache() ||
                     self.data.mtptReadCache1() === storageDriver.mountpointFOC()
                    ) && $.inArray('readcache1', fields) === -1) {
                    valid = false;
                    fields.push('readcache1');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.cachefs') }));
                }
                if ((self.data.mtptReadCache2() === storageDriver.mountpointReadCache1() ||
                     self.data.mtptReadCache2() === storageDriver.mountpointReadCache2() ||
                     self.data.mtptReadCache2() === storageDriver.mountpointWriteCache() ||
                     self.data.mtptReadCache2() === storageDriver.mountpointFOC()
                    ) && $.inArray('readcache2', fields) === -1) {
                    valid = false;
                    fields.push('readcache2');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.cachefs') }));
                }
                if ((self.data.mtptWriteCache() === storageDriver.mountpointReadCache1() ||
                     self.data.mtptWriteCache() === storageDriver.mountpointReadCache2() ||
                     self.data.mtptWriteCache() === storageDriver.mountpointWriteCache() ||
                     self.data.mtptWriteCache() === storageDriver.mountpointFOC()
                    ) && $.inArray('writecache', fields) === -1) {
                    valid = false;
                    fields.push('writecache');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.cachefs') }));
                }
                if ((self.data.mtptFOC() === storageDriver.mountpointReadCache1() ||
                     self.data.mtptFOC() === storageDriver.mountpointReadCache2() ||
                     self.data.mtptFOC() === storageDriver.mountpointWriteCache() ||
                     self.data.mtptFOC() === storageDriver.mountpointFOC()
                    ) && $.inArray('foc', fields) === -1) {
                    valid = false;
                    fields.push('foc');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.cachefs') }));
                }
                if (self.data.mtptBFS() === storageDriver.mountpointBFS() && $.inArray('bfs', fields) === -1 && (self.data.backend() === 'local' || self.data.backend() === 'distributed')) {
                    valid = false;
                    fields.push('bfs');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.bfs') }));
                }
                if (self.data.mtptMD() === storageDriver.mountpointMD() && $.inArray('md', fields) === -1) {
                    valid = false;
                    fields.push('md');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.mdfs') }));
                }
                if (self.data.mtptTemp() === storageDriver.mountpointTemp() && $.inArray('temp', fields) === -1) {
                    valid = false;
                    fields.push('temp');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.tempfs') }));
                }
                if ((self.data.mtptBFS() === storageDriver.mountpointMD() ||
                     self.data.mtptBFS() === storageDriver.mountpointReadCache1() ||
                     self.data.mtptBFS() === storageDriver.mountpointReadCache2() ||
                     self.data.mtptBFS() === storageDriver.mountpointWriteCache() ||
                     self.data.mtptBFS() === storageDriver.mountpointFOC() ||
                     self.data.mtptBFS() === storageDriver.mountpointTemp()) && $.inArray('bfs', fields) === -1) {
                    valid = false;
                    fields.push('bfs');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.bfsexclusive'));
                }
                return true;
            });
            if ((self.data.mtptBFS() === self.data.mtptReadCache1() ||
                 self.data.mtptBFS() === self.data.mtptReadCache2() ||
                 self.data.mtptBFS() === self.data.mtptMD() ||
                 self.data.mtptBFS() === self.data.mtptTemp()) && $.inArray('bfs', fields) === -1 &&
                    (self.data.backend() === 'local' || self.data.backend() === 'distributed')) {
                valid = false;
                fields.push('bfs');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.bfsnotshared'));
            }
            if (!self.data.allowVPool() && $.inArray('vpool', fields) === -1) {
                valid = false;
                fields.push('vpool');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.vpoolnotallowed'));
            }
            $.each(self.data.mtptReadCaches(), function(index, cache) {
                if ($.inArray('readcache', fields) === -1 && cache.match(self.mountpointRegex) === null) {
                    valid = false;
                    fields.push('readcache');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.invalidmtpt', { what: $.t('ovs:generic.cachefs') }));
                }
            });
            $.each(self.data.mtptWriteCaches(), function(index, cache) {
                if ($.inArray('writecache', fields) === -1 && cache.match(self.mountpointRegex) === null) {
                    valid = false;
                    fields.push('writecache');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.invalidmtpt', { what: $.t('ovs:generic.cachefs') }));
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
    };
});
