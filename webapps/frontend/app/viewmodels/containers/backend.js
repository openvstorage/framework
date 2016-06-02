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
        self.backendType = ko.observable();
        self.domains     = ko.observableArray([]);

        // Observables
        self.backendTypeGuid = ko.observable();
        self.domainGuids     = ko.observableArray([]);
        self.edit            = ko.observable(false);
        self.guid            = ko.observable(guid);
        self.loaded          = ko.observable(false);
        self.loading         = ko.observable(false);
        self.name            = ko.observable();
        self.status          = ko.observable();
        self.saving          = ko.observable(false);

        // Functions
        self.fillData = function(data) {
            if (self.edit() === true) {
                self.loading(false);
                return;
            }
            self.name(data.name);
            generic.trySet(self.backendTypeGuid, data, 'backend_type_guid');
            self.status(data.status !== undefined ? data.status.toLowerCase() : 'unknown');
            if (data.hasOwnProperty('regular_domains')) {
                self.domainGuids(data.regular_domains);
            }

            self.loaded(true);
            self.loading(false);
        };
        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                if (generic.xhrCompleted(self.loadHandle)) {
                    self.loadHandle = api.get('backends/' + self.guid(), { queryparams: { contents: '_relations,regular_domains' } })
                        .done(function(data) {
                            self.fillData(data);
                            deferred.resolve(data);
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
                self.saving(true);
                var data = {
                    domain_guids: self.domainGuids()
                };
                api.post('backends/' + self.guid() + '/set_domains', { data: data })
                    .done(function() {
                        generic.alertSuccess($.t('ovs:generic.saved'), $.t('ovs:backends.save.success'));
                        deferred.resolve();
                    })
                    .fail(function() {
                        generic.alertError($.t('ovs:generic.error'), $.t('ovs:backends.save.failure'));
                        deferred.reject();
                    })
                    .always(function() {
                        self.edit(false);
                        self.saving(false);
                    });
            }).promise();
        };
    };
});
