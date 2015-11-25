// Copyright 2015 iNuron NV
//
// Licensed under the Open vStorage Modified Apache License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
