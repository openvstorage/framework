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
    'viewmodels/containers/user/user', 'viewmodels/containers/user/client'
], function($, ko, generic, shared, api, User, Client) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared        = shared;
        self.parentSave    = undefined;
        self.refresh       = 0;
        self.usersHandle   = undefined;
        self.clientsHandle = undefined;
        self.usersLoaded   = false;
        self.clientsLoaded = false;
        self.subscription  = undefined;

        // Observables
        self.dataLoading     = ko.observable(false);
        self.widgetActivated = ko.observable(false);
        self.liveRights      = ko.observable();
        self.workingRights   = ko.observable();
        self.users           = ko.observableArray([]);
        self.clients         = ko.observableArray([]);
        self.userMap         = ko.observable({});
        self.clientMap       = ko.observable({});
        self.userClientMap   = ko.observable({});
        self.editMode        = ko.observable(false);

        // Computed
        self.rights = ko.computed(function() {
            var rights = self.editMode() ? self.workingRights() : self.liveRights();
            return rights === undefined ? {} : rights;
        });
        self.dirty = ko.computed(function() {
            return generic.objectEquals(self.workingRights(), self.liveRights());
        });
        self.userRights = ko.computed(function() {
            return self.rights()['users'];
        });
        self.clientRights = ko.computed(function() {
            return self.rights()['clients'];
        });

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
                if (generic.xhrCompleted(self.clientsHandle)) {
                    var options = {sort: 'name',
                                   contents: '_relations'};
                    self.clientsHandle = api.get('clients', { queryparams: options })
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
                            self.userClientMap.notifySubscribers();
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
        self.startEdit = function() {
            self.workingRights($.extend(true, {}, self.liveRights()));
            self.editMode(true);
        };
        self.cancelEdit = function() {
            self.editMode(false);
        };
        self.save = function() {
            self.parentSave(self.workingRights())
                .then(function() {
                    self.liveRights(self.workingRights());
                })
                .always(function() {
                    self.editMode(false);
                })
        };
        self.swap = function(type, guid, grant) {
            if (self.editMode() === false) {
                return;
            }
            var rights = self.rights();
            if (type === 'clients' && grant === true) {
                var userGuid = self.clientMap()[guid].userGuid();
                if (rights['users'][userGuid] === false) {
                    return;
                }
            }
            if (rights[type][guid] === grant) {
                delete rights[type][guid];
            } else {
                rights[type][guid] = grant;
            }
            if (type === 'users' && grant === false && self.userClientMap()[guid] !== undefined) {
                $.each(self.userClientMap()[guid](), function(index, clientGuid) {
                    if (rights['clients'][clientGuid] === true) {
                        delete rights['clients'][clientGuid];
                    }
                });
            }
            self.rights.notifySubscribers();
        };

        // Durandal
        self.activate = function(settings) {
            if (!settings.hasOwnProperty('rights')) {
                throw 'Rights should be specified';
            }
            if (!settings.hasOwnProperty('save')) {
                throw 'A save function should be specified'
            }

            self.subscription = self.liveRights.subscribe(function() {
                if (self.liveRights() !== undefined) {
                    if (self.liveRights().hasOwnProperty('users')) {
                        self.loadUsers();
                    }
                    if (self.liveRights().hasOwnProperty('clients')) {
                        self.loadClients();
                    }
                    self.subscription.dispose();
                }
            });

            self.liveRights(settings.rights);
            self.parentSave = settings.save;
            self.refresh = parseInt(generic.tryGet(settings, 'refreshInterval', '5000'), 10);

            self.userClientMap({});
            self.widgetActivated(true);
        };
    };
});
