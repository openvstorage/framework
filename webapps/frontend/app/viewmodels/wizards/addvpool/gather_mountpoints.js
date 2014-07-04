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
        self.data = data;

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, reasons = [], fields = [];
            if (self.data.mtptCache() === '/' && $.inArray('cache', fields) === -1) {
                valid = false;
                fields.push('cache');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.cachefs') }));
            }
            if (self.data.mtptBFS() === '/' && $.inArray('bfs', fields) === -1 && (self.data.backend() === 'LOCAL' || self.data.backend() === 'DISTRIBUTED')) {
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
            $.each(self.data.storageRouters(), function(index, storageRouter) {
                if (self.data.target() !== undefined && storageRouter.storageRouterID() === (self.data.name() + self.data.target().machineid())) {
                    return true;
                }
                if (self.data.mtptCache() === storageRouter.mountpointCache() && $.inArray('cache', fields) === -1) {
                    valid = false;
                    fields.push('cache');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.cachefs') }));
                }
                if (self.data.mtptBFS() === storageRouter.mountpointBFS() && $.inArray('bfs', fields) === -1 && (self.data.backend() === 'LOCAL' || self.data.backend() === 'DISTRIBUTED')) {
                    valid = false;
                    fields.push('bfs');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.bfs') }));
                }
                if (self.data.mtptMD() === storageRouter.mountpointMD() && $.inArray('md', fields) === -1) {
                    valid = false;
                    fields.push('md');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.mdfs') }));
                }
                if (self.data.mtptTemp() === storageRouter.mountpointTemp() && $.inArray('temp', fields) === -1) {
                    valid = false;
                    fields.push('temp');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.tempfs') }));
                }
                if ((self.data.mtptBFS() === storageRouter.mountpointCache() || self.data.mtptBFS() === storageRouter.mountpointMD() ||
                        self.data.mtptBFS() === storageRouter.mountpointTemp()) && $.inArray('bfs', fields) === -1) {
                    valid = false;
                    fields.push('bfs');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.bfsexclusive'));
                }
                if ($.inArray(self.data.vRouterPort(), [storageRouter.port() - 1, storageRouter.port(), storageRouter.port() + 1]) !== -1 && $.inArray('port', fields) === -1) {
                    valid = false;
                    fields.push('port');
                    reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.portinuse'));
                }
                return true;
            });
            if ((self.data.mtptBFS() === self.data.mtptCache() || self.data.mtptBFS() === self.data.mtptMD() ||
                    self.data.mtptBFS() === self.data.mtptTemp()) && $.inArray('bfs', fields) === -1 &&
                    (self.data.backend() === 'LOCAL' || self.data.backend() === 'DISTRIBUTED')) {
                valid = false;
                fields.push('bfs');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.bfsnotshared'));
            }
            if (!self.data.allowVPool() && $.inArray('vpool', fields) === -1) {
                valid = false;
                fields.push('vpool');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.vpoolnotallowed'));
            }
            if (!self.data.mtptCache.valid()) {
                valid = false;
                fields.push('cache');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.invalidmtpt', { what: $.t('ovs:generic.cachefs') }));
            }
            if (!self.data.mtptTemp.valid()) {
                valid = false;
                fields.push('temp');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.invalidmtpt', { what: $.t('ovs:generic.tempfs') }));
            }
            if (!self.data.mtptBFS.valid() && (self.data.backend() === 'LOCAL' || self.data.backend() === 'DISTRIBUTED')) {
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
    };
});
