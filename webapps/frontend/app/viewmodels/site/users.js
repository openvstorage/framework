// Copyright 2014 CloudFounders NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define([
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/user', '../containers/client', '../wizards/changepassword/index'
], function($, app, dialog, ko, shared, generic, Refresher, api, User, Client, ChangePasswordWizard) {
    "use strict";
    return function () {
        var self = this;

        // Variables
        self.widgets       = [];
        self.shared        = shared;
        self.guard         = { authenticated: true };
        self.refresher     = new Refresher();
        self.userHeaders   = [
            { key: 'username', value: $.t('ovs:generic.username'), width: undefined },
            { key: undefined,  value: $.t('ovs:generic.actions'),  width: 60        }
        ];
        self.clientHeaders = [
            { key: 'name',         value: $.t('ovs:generic.name'),               width: 250       },
            { key: 'clientid',     value: $.t('ovs:users.clients.clientid'),     width: 300       },
            { key: 'clientsecret', value: $.t('ovs:users.clients.clientsecret'), width: 300       },
            { key: 'granttype',    value: $.t('ovs:users.clients.granttype'),    width: undefined },
            { key: undefined,      value: $.t('ovs:generic.actions'),            width: 60        }
        ];

        // Observables
        self._selectedUserGuid  = ko.observable();
        self.users              = ko.observableArray([]);
        self.usersInitialLoad   = ko.observable(true);
        self.clients            = ko.observableArray([]);
        self.clientsInitialLoad = ko.observable(true);
        self.newClient          = ko.observable(new Client());

        // Computed
        self.selectedUserGuid = ko.computed({
            write: function(guid) {
                self._selectedUserGuid(guid);
                self.fetchClients();
            },
            read: function() {
                return self._selectedUserGuid();
            }
        });

        // Handles
        self.loadUsersHandle      = undefined;
        self.refreshUsersHandle   = {};
        self.loadClientsHandle    = undefined;
        self.refreshClientsHandle = {};

        // Functions
        self.fetchUsers = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadUsersHandle)) {
                    var options = {
                        sort: 'username',
                        contents: ''
                    };
                    self.loadUsersHandle = api.get('users', undefined, options)
                        .done(function(data) {
                            var guids = [], udata = {};
                            $.each(data, function(i, item) {
                                guids.push(item.guid);
                                udata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.users,
                                function(guid) {
                                    var user = new User(guid);
                                    if ($.inArray(guid, guids) !== -1) {
                                        user.fillData(udata[guid]);
                                    }
                                    user.loading(true);
                                    return user;
                                }, 'guid'
                            );
                            if (self.selectedUserGuid() === undefined || $.inArray(self.selectedUserGuid(), guids) === -1) {
                                self.selectedUserGuid(guids[0]);
                            }
                            self.usersInitialLoad(false);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.refreshUsers = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.refreshUsersHandle[page])) {
                    var options = {
                        sort: 'username',
                        page: page,
                        contents: '_relations'
                    };
                    self.refreshUsersHandle[page] = api.get('users', {}, options)
                        .done(function(data) {
                            var guids = [], udata = {};
                            $.each(data, function(i, item) {
                                guids.push(item.guid);
                                udata[item.guid] = item;
                            });
                            $.each(self.users(), function(i, user) {
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
        self.fetchClients = function() {
            return $.Deferred(function(deferred) {
                if (self.selectedUserGuid() !== undefined && generic.xhrCompleted(self.loadClientsHandle)) {
                    $.each(self.clients(), function(i, client) {
                        client.loading(true);
                    });
                    var options = {
                        sort: 'name',
                        contents: '',
                        userguid: self.selectedUserGuid()
                    };
                    self.loadClientsHandle = api.get('clients', undefined, options)
                        .done(function(data) {
                            var guids = [], cdata = {};
                            $.each(data, function(i, item) {
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
                                    return client;
                                }, 'guid'
                            );
                            self.clientsInitialLoad(false);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.refreshClients = function(page) {
            return $.Deferred(function(deferred) {
                if (self.selectedUserGuid() !== undefined && generic.xhrCompleted(self.refreshClientsHandle[page])) {
                    var options = {
                        sort: 'name',
                        page: page,
                        contents: '_relations',
                        userguid: self.selectedUserGuid()
                    };
                    self.refreshClientsHandle[page] = api.get('clients', {}, options)
                        .done(function(data) {
                            var guids = [], cdata = {};
                            $.each(data, function(i, item) {
                                guids.push(item.guid);
                                cdata[item.guid] = item;
                            });
                            $.each(self.clients(), function(i, client) {
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
        self.changePassword = function(guid) {
            $.each(self.users(), function(i, user) {
                if (user.guid() === guid) {
                    dialog.show(new ChangePasswordWizard({
                        modal: true,
                        user: user
                    }));
                }
            });
        };
        self.deleteClient = function(guid) {
            $.each(self.clients(), function(i, client) {
                if (client.guid() === guid) {
                    app.showMessage(
                        $.t('ovs:users.clients.delete', { what: client.name() }),
                        $.t('ovs:generic.areyousure'),
                        [$.t('ovs:generic.no'), $.t('ovs:generic.yes')]
                    )
                    .done(function(answer) {
                        if (answer === $.t('ovs:generic.yes')) {
                            api.del('clients/' + guid)
                                .done(function () {
                                    generic.alertSuccess(
                                        $.t('ovs:users.clients.complete'),
                                        $.t('ovs:users.clients.deletesuccess')
                                    );
                                    self.clients.remove(client);
                                    self.fetchClients();
                                })
                                .fail(function (error) {
                                    error = $.parseJSON(error.responseText);
                                    generic.alertError(
                                        $.t('ovs:generic.error'),
                                        $.t('ovs:users.clients.deletefailed', { why: error.detail })
                                    );
                                });
                        }
                    });
                }
            });
        };
        self.saveClient = function() {
            api.post('clients', {
                name: self.newClient().name(),
                ovs_type: 'USER',
                user_guid: self.selectedUserGuid()
            })
                .done(function() {
                    generic.alertSuccess(
                        $.t('ovs:users.clients.complete'),
                        $.t('ovs:users.clients.addsuccess')
                    );
                    self.fetchClients();
                })
                .fail(function(error) {
                    error = $.parseJSON(error.responseText);
                    generic.alertError(
                        $.t('ovs:generic.error'),
                        $.t('ovs:users.clients.addfailed', { why: error.detail })
                    );
                })
                .always(function() {
                    self.newClient(new Client());
                });
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(function() {
                self.fetchClients();
                self.fetchUsers();
            }, 5000);
            self.refresher.start();
            self.fetchClients().then(function() {
                self.refreshClients(1);
            });
            self.fetchUsers().then(function() {
                self.refreshUsers(1);
            });
        };
        self.deactivate = function() {
            $.each(self.widgets, function(i, item) {
                item.deactivate();
            });
            self.refresher.stop();
        };
    };
});
