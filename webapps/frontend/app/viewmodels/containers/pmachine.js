// Copyright 2014 iNuron NV
//
// Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/OVS_NON_COMMERCIAL
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
        self.loadConfigured = undefined;

        // Observables
        self.edit              = ko.observable(false);
        self.loading           = ko.observable(false);
        self.loaded            = ko.observable(false);
        self.guid              = ko.observable(guid);
        self.name              = ko.observable();
        self.ipAddress         = ko.observable();
        self.hvtype            = ko.observable();
        self.mgmtCenterGuid    = ko.observable();
        self.backupValue       = ko.observable();
        self.isConfigured      = ko.observable(false);
        self.isVPoolConfigured = ko.observable({});

        // Functions
        self.fillData = function(data) {
            if (!self.edit()) {
                self.name(data.name);
                self.hvtype(data.hvtype);
                self.ipAddress(data.ip);
                if (data.hasOwnProperty('mgmtcenter_guid')) {
                    self.mgmtCenterGuid(data.mgmtcenter_guid);
                }
            }
            self.loaded(true);
            self.loading(false);
        };
        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                api.get('pmachines/' + self.guid())
                    .done(function(data) {
                        self.fillData(data);
                        deferred.resolve();
                    })
                    .fail(deferred.reject)
                    .always(function() {
                        self.loading(false);
                    });
            }).promise();
        };
        self.loadHostConfigurationState = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadConfigured)) {
                    self.loadConfigured = api.get('pmachines/' + self.guid() + '/is_host_configured')
                        .then(self.shared.tasks.wait)
                        .done(function(data) {
                            self.isConfigured(data);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.loadVPoolConfigurationState = function(vpoolGuid) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadConfigured)) {
                    self.loadConfigured = api.get('pmachines/' + self.guid() + '/is_host_configured_for_vpool', {
                        queryparams: { vpool_guid: vpoolGuid }
                    })
                        .then(self.shared.tasks.wait)
                        .done(function(data) {
                            var configuredVPools = self.isVPoolConfigured();
                            configuredVPools[vpoolGuid] = data;
                            self.isVPoolConfigured(configuredVPools);
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
