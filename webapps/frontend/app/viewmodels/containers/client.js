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
    'ovs/generic', 'ovs/api'
], function($, ko, generic, api) {
    "use strict";
    return function(guid) {
        var self = this;

        // Handles
        self.loadHandle = undefined;

        // Observables
        self.loading       = ko.observable(false);
        self.loaded        = ko.observable(false);
        self.guid          = ko.observable(guid);
        self.name          = ko.observable();
        self.clientSecret  = ko.observable();
        self.grantType     = ko.observable();
        self.ovsType       = ko.observable();
        self.userGuid      = ko.observable();
        self.roleJunctions = ko.observableArray([]);

        // Functions
        self.fillData = function(data) {
            self.name(data.name);
            self.clientSecret(data.client_secret);
            self.grantType(data.grant_type);
            self.ovsType(data.ovs_type);
            generic.trySet(self.userGuid, data, 'user_guid');
            generic.trySet(self.roleJunctions, data, 'roles_guids');

            self.loaded(true);
            self.loading(false);
        };
        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                if (generic.xhrCompleted(self.loadHandle)) {
                    self.loadHandle = api.get('clients/' + self.guid())
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
    };
});
