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
    'ovs/api'
], function($, ko, api) {
    "use strict";
    return function(guid) {
        var self = this;

        // Handles
        self.loadHandle = undefined;

        // Observables
        self.loading    = ko.observable(false);
        self.loaded     = ko.observable(false);
        self.guid       = ko.observable(guid);
        self.name       = ko.observable();
        self.ipAddress  = ko.observable();
        self.username   = ko.observable();
        self.port       = ko.observable();
        self.centerType = ko.observable();
        self.hosts      = ko.observable({});

        // Functions
        self.fillData = function(data) {
            self.name(data.name);
            self.centerType(data.type);
            self.username(data.username);
            self.port(data.port);
            self.ipAddress(data.ip);
            if (data.hasOwnProperty('hosts')) {
                self.hosts(data.hosts);
            }
            self.loaded(true);
            self.loading(false);
        };
        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                api.get('mgmtcenters/' + self.guid())
                    .done(function(data) {
                        self.fillData(data);
                        deferred.resolve();
                    })
                    .fail(deferred.reject)
                    .always(function() {
                        self.loading(false);
                    });
            }).promise();
        }
    };
});
