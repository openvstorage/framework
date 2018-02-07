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

        // External dependencies
        self.group   = ko.observable();
        self.clients = ko.observableArray([]);

        // Observables
        self.edit        = ko.observable(false);
        self.loading     = ko.observable(false);
        self.loaded      = ko.observable(false);
        self.guid        = ko.observable(guid);
        self.active      = ko.observable();
        self.username    = ko.observable();
        self.groupGuid   = ko.observable();
        self.backupValue = ko.observable();

        // Functions
        self.fillData = function(data) {
            self.username(data.username);
            self.active(data.is_active);
            generic.trySet(self.groupGuid, data, 'group_guid');

            self.loaded(true);
            self.loading(false);
        };
        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                if (generic.xhrCompleted(self.loadHandle)) {
                    self.loadHandle = api.get('users/' + self.guid())
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
        self.save = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                api.patch('users/' + self.guid(), {
                        data: { group_guid: self.groupGuid() },
                        queryparams: { contents: '_relations' }
                    })
                    .done(function() {
                        generic.alertSuccess(
                            $.t('ovs:users.save.complete'),
                            $.t('ovs:users.save.success', { what: self.username() })
                        );
                        self.loading(false);
                        deferred.resolve();
                    })
                    .fail(function(error) {
                        error = generic.extractErrorMessage(error);
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:users.save.failed', {
                                what: self.username(),
                                why: error
                            })
                        );
                        self.loading(false);
                        deferred.reject();
                    });
            }).promise();
        };
    };
});
