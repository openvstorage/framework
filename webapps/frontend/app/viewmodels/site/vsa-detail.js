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
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher',
    '../containers/vmachine', '../containers/pmachine', '../containers/vpool'
], function($, app, dialog, ko, shared, generic, Refresher, VMachine, PMachine, VPool) {
    "use strict";
    return function() {
        var self = this;

        // System
        self.shared    = shared;
        self.guard     = { authenticated: true };
        self.refresher = new Refresher();
        self.widgets   = [];

        // Data
        self.VSA           = ko.observable();
        self.pMachineCache = {};
        self.vPoolCache    = {};
        self.vMachineCache = {};

        // Functions
        self.load = function() {
            return $.Deferred(function(deferred) {
                var vsa = self.VSA();
                $.when.apply($, [
                        vsa.load(),
                        vsa.fetchServedChildren()
                    ])
                    .done(function() {
                        var pMachineGuid = vsa.pMachineGuid(), pm;
                        if (pMachineGuid && (vsa.pMachine() === undefined || vsa.pMachine().guid() !== pMachineGuid)) {
                            if (!self.pMachineCache.hasOwnProperty(pMachineGuid)) {
                                pm = new PMachine(pMachineGuid);
                                pm.load();
                                self.pMachineCache[pMachineGuid] = pm;
                            }
                            vsa.pMachine(self.pMachineCache[pMachineGuid]);
                        }
                        // Move child guids to the observables for easy display
                        vsa.vpools(vsa.vPoolGuids);
                        vsa.vMachines(vsa.vMachineGuids);
                    })
                    .always(deferred.resolve);
            }).promise();
        };

        // Durandal
        self.activate = function(mode, guid) {
            self.VSA(new VMachine(guid));
            self.refresher.init(self.load, 5000);
            self.refresher.run();
            self.refresher.start();
            self.shared.footerData(self.VSA);
        };
        self.deactivate = function() {
            var i;
            for (i = 0; i < self.widgets.length; i += 2) {
                self.widgets[i].deactivate();
            }
            self.refresher.stop();
            self.shared.footerData(ko.observable());
        };
    };
});
