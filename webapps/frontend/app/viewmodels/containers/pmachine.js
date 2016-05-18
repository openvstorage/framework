// Copyright (C) 2016 iNuron NV
//
// This file is part of Open vStorage Open Source Edition (OSE),
// as available from
//
//      http://www.openvstorage.org and
//      http://www.openvstorage.com.
//
// This file is free software; you can redistribute it and/or modify it
// under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
// as published by the Free Software Foundation, in version 3 as it comes
// in the LICENSE.txt file of the Open vStorage OSE distribution.
//
// Open vStorage is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY of any kind.
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
