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
        self.backendGuids        = ko.observableArray([]);
        self.disabled            = ko.observable(false);
        self.edit                = ko.observable(false);
        self.existingDomainNames = undefined;
        self.guid                = ko.observable(guid);
        self.loaded              = ko.observable(false);
        self.loading             = ko.observable(false);
        self.name                = ko.observable('').extend({removeWhiteSpaces: null});
        self.storageRouterGuids  = ko.observableArray([]);
        self.storageRouterLayout = ko.observable();
        self.vdiskDtlGuids       = ko.observableArray([]);

        // Computed
        self.canSave = ko.computed(function() {
            var names = [];
            if (self.existingDomainNames !== undefined) {
                names = self.existingDomainNames().slice();
                if (self.guid() !== undefined) { // Remove yourself when editing the name
                    names.remove(self.name().toLowerCase());
                }
            }
            return self.name() !== undefined && self.name() !== '' && !names.contains(self.name().toLowerCase()) && self.name().length < 31;
        });
        self.canDelete = ko.computed(function() {
            return self.storageRouterGuids() !== undefined && self.storageRouterGuids().length === 0 &&
                   self.backendGuids() !== undefined && self.backendGuids().length === 0 &&
                   self.vdiskDtlGuids() !== undefined && self.vdiskDtlGuids().length === 0;
        });

        // Functions
        self.fillData = function(data) {
            self.name(data.name);
            self.backendGuids(data.backends_guids);
            self.vdiskDtlGuids(data.vdisks_dtl_guids);
            self.storageRouterGuids(data.storagerouters_guids);
            if (data.hasOwnProperty('storage_router_layout')) {
                self.storageRouterLayout(data.storage_router_layout);
            }
            self.loaded(true);
            self.loading(false);
        };
        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                if (generic.xhrCompleted(self.loadHandle)) {
                    self.loadHandle = api.get('domains/' + self.guid())
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
                if (self.guid() === undefined) {
                    api.post('domains', {data: {name: self.name()}})
                        .done(function (data) {
                            deferred.resolve(data.guid);
                        })
                        .fail(deferred.reject)
                        .always(function() {
                            self.edit(false);
                        });
                } else {
                    api.patch('domains/' + self.guid(), {data: {name: self.name()}})
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
