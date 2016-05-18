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
        self.address          = ko.observable('');
        self.city             = ko.observable('');
        self.country          = ko.observable('');
        self.disabled         = ko.observable(false);
        self.edit             = ko.observable(false);
        self.guid             = ko.observable(guid);
        self.loaded           = ko.observable(false);
        self.loading          = ko.observable(false);
        self.name             = ko.observable('');
        self.primarySRGuids   = ko.observableArray([]);
        self.secondarySRGuids = ko.observableArray([]);

        // Computed
        self.canSave = ko.computed(function() {
            return !(self.name() === undefined || self.name() === '');
        });
        self.canDelete = ko.computed(function() {
            return self.primarySRGuids().length === 0 && self.secondarySRGuids().length === 0;
        });

        // Functions
        self.fillData = function(data) {
            self.name(data.name);
            generic.trySet(self.city, data, 'city');
            generic.trySet(self.address, data, 'address');
            generic.trySet(self.country, data, 'country');
            if (data.hasOwnProperty('primary_storagerouters_guids')) {
                self.primarySRGuids(data.primary_storagerouters_guids);
            }
            if (data.hasOwnProperty('secondary_storagerouters_guids')) {
                self.secondarySRGuids(data.secondary_storagerouters_guids);
            }

            self.loaded(true);
            self.loading(false);
        };
        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                if (generic.xhrCompleted(self.loadHandle)) {
                    self.loadHandle = api.get('failure_domains/' + self.guid())
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
                var data = {
                    name: self.name(),
                    address: self.address(),
                    city: self.city(),
                    country: self.country()
                };
                if (self.guid() === undefined) {
                    api.post('failure_domains', { data: data })
                        .done(function (data) {
                            deferred.resolve(data.guid);
                        })
                        .fail(deferred.reject)
                        .always(function() {
                            self.edit(false);
                        });
                } else {
                    api.patch('failure_domains/' + self.guid(), { data: data })
                        .done(function (data) {
                            deferred.resolve(data.guid);
                        })
                        .fail(deferred.reject)
                        .always(function() {
                            self.edit(false);
                        });
                }
            }).promise();
        };
    };
});
