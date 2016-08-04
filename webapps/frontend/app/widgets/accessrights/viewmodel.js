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
    'jquery', 'knockout', 'ovs/generic', 'ovs/shared', 'ovs/api',
    '../../viewmodels/containers/user', '../../viewmodels/containers/client'
], function($, ko, generic, shared, api, User, Client) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared        = shared;
        self.save          = undefined;
        self.refresh       = 0;
        self.usersHandle   = undefined;
        self.clientsHandle = undefined;

        // Observables
        self.dataLoading     = ko.observable(false);
        self.widgetActivated = ko.observable(false);
        self.rights          = ko.observable();
        self.originalRights  = ko.observable();
        self.users           = ko.observableArray([]);
        self.clients         = ko.observableArray([]);
        self.userMap         = ko.observable({});
        self.clientMap       = ko.observable({});
        self.userClientMap   = ko.observable({});

        // Computed


        // Functions
        self.loadUsers = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.usersHandle)) {
                    var options = {sort: 'username',
                                   contents: '_relations'};
                    self.usersHandle = api.get('users', { queryparams: options })
                        .done(function(data) {
                            var guids = [], udata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                udata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.users,
                                function(guid) {
                                    var user = new User(guid);
                                    self.userMap()[guid] = user;
                                    return user;
                                }, 'guid'
                            );
                            $.each(self.users(), function(index, user) {
                                if ($.inArray(user.guid(), guids) !== -1) {
                                    user.fillData(udata[user.guid()]);
                                }
                            });
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.loadClients = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.usersHandle)) {
                    var options = {sort: 'name',
                                   contents: '_relations'};
                    self.usersHandle = api.get('clients', { queryparams: options })
                        .done(function(data) {
                            var guids = [], cdata = {}, map = self.userClientMap();
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                cdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.clients,
                                function(guid) {
                                    var client = new Client(guid);
                                    if ($.inArray(guid, guids) !== -1) {
                                        client.fillData(cdata[guid]);
                                    }
                                    self.clientMap()[guid] = client;
                                    if (!map.hasOwnProperty(client.userGuid())) {
                                        map[client.userGuid()] = ko.observableArray([]);
                                    }
                                    if (!map[client.userGuid()]().contains(guid)) {
                                        map[client.userGuid()].push(guid);
                                    }
                                    return client;
                                }, 'guid'
                            );
                            $.each(self.clients(), function(index, client) {
                                if ($.inArray(client.guid(), guids) !== -1) {
                                    client.fillData(cdata[client.guid()]);
                                }
                            });
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.resolve();
                }
            }).promise();
        };

        // Durandal
        self.activate = function(settings) {
            if (!settings.hasOwnProperty('rights')) {
                throw 'Rights should be specified';
            }
            if (!settings.hasOwnProperty('save')) {
                throw 'A save function should be specified'
            }
            self.rights = settings.rights;
            self.originalRights($.extend(true, {}, settings.rights()));
            self.save = settings.save;
            self.refresh = parseInt(generic.tryGet(settings, 'refreshInterval', '5000'), 10);

            self.userClientMap({});
            var calls = [];
            if (self.rights().hasOwnProperty('users')) {
                calls.push(self.loadUsers());
            }
            if (self.rights().hasOwnProperty('clients')) {
                calls.push(self.loadClients());
            }
            return $.when.apply($, calls)
                .then(function() {
                    self.widgetActivated(true);
                });
        };
    };
});
