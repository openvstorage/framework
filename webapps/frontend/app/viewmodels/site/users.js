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
    '../containers/user', '../containers/client', '../containers/group', '../containers/role',
    '../wizards/changepassword/index'
], function($, app, dialog, ko, shared, generic, Refresher, api, User, Client, Group, Role, ChangePasswordWizard) {
    "use strict";
    return function () {
        var self = this;

        // Variables
        self.widgets       = [];
        self.shared        = shared;
        self.guard         = { authenticated: true };
        self.refresher     = new Refresher();
        self.userHeaders   = [
            { key: 'username', value: $.t('ovs:generic.username'), width: 250       },
            { key: 'group',    value: $.t('ovs:generic.group'),    width: undefined },
            { key: undefined,  value: $.t('ovs:generic.actions'),  width: 60        }
        ];
        self.clientHeaders = [
            { key: 'name',         value: $.t('ovs:generic.name'),               width: 240       },
            { key: 'clientid',     value: $.t('ovs:users.clients.clientid'),     width: 330       },
            { key: 'clientsecret', value: $.t('ovs:users.clients.clientsecret'), width: 270       },
            { key: 'granttype',    value: $.t('ovs:users.clients.roles'),        width: undefined },
            { key: undefined,      value: $.t('ovs:generic.actions'),            width: 60        }
        ];

        // Observables
        self._selectedUserGuid  = ko.observable();
        self.users              = ko.observableArray([]);
        self.clients            = ko.observableArray([]);
        self.roles              = ko.observableArray([]);
        self.rolesInitialLoad   = ko.observable(true);
        self.roleMapping        = ko.observable({});
        self.groups             = ko.observableArray([]);
        self.groupsInitialLoad  = ko.observable(true);
        self.groupMapping       = ko.observable({});

        // Computed
        self.selectedUserGuid = ko.computed({
            write: function(guid) {
                self._selectedUserGuid(guid);
                self.newUser(self.buildUser());
                self.newClient(self.buildClient());
            },
            read: function() {
                return self._selectedUserGuid();
            }
        });
        self.selectedUser = ko.computed(function() {
            var user;
            $.each(self.users(), function(i, currentUser) {
                if (currentUser.guid() === self._selectedUserGuid()) {
                    user = currentUser;
                }
            });
            return user;
        });
        self.availableRoles = ko.computed(function() {
            var availableRoles = [],
                user = self.selectedUser(),
                group = user !== undefined ? user.group() : undefined;
            if (group !== undefined) {
                $.each(group.roleJunctions(), function(i, rJunctionGuid) {
                    $.each(self.roles(), function(j, role) {
                        $.each(role.groupJunctions(), function(k, gJunctionGuid) {
                            if (rJunctionGuid === gJunctionGuid) {
                                availableRoles.push(role);
                            }
                        });
                    });
                });
            }
            return availableRoles;
        });

        // Handles
        self.usersHandle      = {};
        self.clientsHandle    = {};
        self.loadGroupsHandle = undefined;
        self.loadRolesHandles = undefined;

        // Functions
        self.buildUser = function() {
            var user = new User();
            user.group = ko.computed({
                write: function(group) {
                    if (group === undefined) {
                        this.groupGuid(undefined);
                    } else {
                        this.groupGuid(group.guid());
                    }
                },
                read: function() {
                    if (self.groupMapping().hasOwnProperty(this.groupGuid())) {
                        return self.groupMapping()[this.groupGuid()];
                    }
                    return undefined;
                },
                owner: user
            });
            return user;
        };
        self.buildClient = function() {
            var client = new Client();
            client.roles = ko.observableArray([]);
            client.roleGuids = ko.computed(function() {
                var guids = [];
                $.each(this.roles(), function(i, role) {
                    guids.push(role.guid());
                });
                return guids;
            }, client);
            return client;
        };
        self.loadUsers = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.usersHandle[page])) {
                    var options = {
                        sort: 'username',
                        contents: '_relations'
                    };
                    self.usersHandle[page] = api.get('users', { queryparams: options })
                        .then(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    var user = new User(guid);
                                    user.group = ko.computed({
                                        write: function (group) {
                                            if (group === undefined) {
                                                this.groupGuid(undefined);
                                            } else {
                                                this.groupGuid(group.guid());
                                            }
                                        },
                                        read: function () {
                                            if (self.groupMapping().hasOwnProperty(this.groupGuid())) {
                                                return self.groupMapping()[this.groupGuid()];
                                            }
                                            return undefined;
                                        },
                                        owner: user
                                    });
                                    return user;
                                },
                                overviewLoader: function(guids) {
                                    if (self.selectedUserGuid() === undefined || $.inArray(self.selectedUserGuid(), guids) === -1) {
                                        self.selectedUserGuid(guids[0]);
                                    }
                                }
                            });
                        })
                        .fail(function() { deferred.reject(); });
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.loadClients = function(page) {
            return $.Deferred(function(deferred) {
                if (self.selectedUserGuid() !== undefined && generic.xhrCompleted(self.clientsHandle[page])) {
                    var options = {
                        sort: 'name',
                        page: page,
                        contents: '_relations',
                        userguid: self.selectedUserGuid(),
                        type: 'USER'
                    };
                    self.clientsHandle[page] = api.get('clients', { queryparams: options })
                        .then(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    var client = new Client(guid);
                                    client.roles = ko.computed(function() {
                                        var roles = [];
                                        $.each(client.roleJunctions(), function(i, rJunctionGuid) {
                                            $.each(self.roles(), function(j, role) {
                                                $.each(role.clientJunctions(), function(k, cJunctionGuid) {
                                                    if (rJunctionGuid === cJunctionGuid) {
                                                        roles.push(role);
                                                    }
                                                });
                                            });
                                        });
                                        return roles;
                                    });
                                    return client;
                                }
                            });
                        })
                        .fail(function() { deferred.reject(); });
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.fetchGroups = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadGroupsHandle)) {
                    var options = {
                        sort: 'name',
                        contents: '_relations'
                    };
                    self.loadGroupsHandle = api.get('groups', { queryparams: options })
                        .done(function(data) {
                            var guids = [], gdata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                gdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.groups,
                                function(guid) {
                                    var group = new Group(guid),
                                        mapping = self.groupMapping();
                                    if (!mapping.hasOwnProperty(guid)) {
                                        mapping[guid] = group;
                                        self.groupMapping(mapping);
                                    }
                                    return group;
                                }, 'guid'
                            );
                            $.each(self.groups(), function(index, group) {
                                if ($.inArray(group.guid(), guids) !== -1) {
                                    group.fillData(gdata[group.guid()]);
                                }
                            });
                            self.groupsInitialLoad(false);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.fetchRoles = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadRolesHandles)) {
                    var options = {
                        sort: 'name',
                        contents: '_relations'
                    };
                    self.loadRolesHandles = api.get('roles', { queryparams: options })
                        .done(function(data) {
                            var guids = [], rdata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                rdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.roles,
                                function(guid) {
                                    var role = new Role(guid),
                                        mapping = self.roleMapping();
                                    if (!mapping.hasOwnProperty(guid)) {
                                        mapping[guid] = role;
                                        self.roleMapping(mapping);
                                    }
                                    return role;
                                }, 'guid'
                            );
                            $.each(self.roles(), function(index, role) {
                                if ($.inArray(role.guid(), guids) !== -1) {
                                    role.fillData(rdata[role.guid()]);
                                }
                            });
                            self.rolesInitialLoad(false);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
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
                data: {
                    name: self.newClient().name(),
                    ovs_type: 'USER',
                    user_guid: self.selectedUserGuid(),
                    role_guids: self.newClient().roleGuids()
                }
            })
                .done(function() {
                    generic.alertSuccess(
                        $.t('ovs:users.clients.complete'),
                        $.t('ovs:users.clients.addsuccess')
                    );
                })
                .fail(function(error) {
                    error = $.parseJSON(error.responseText);
                    generic.alertError(
                        $.t('ovs:generic.error'),
                        $.t('ovs:users.clients.addfailed', { why: error.detail })
                    );
                })
                .always(function() {
                    self.newClient(self.buildClient());
                });
        };
        self.saveUser = function() {
            api.post('users', {
                data: {
                    username: self.newUser().username(),
                    group_guid: self.newUser().group().guid(),
                    is_active: true,
                    password: generic.getTimestamp().toString()
                }
            })
                .done(function() {
                    generic.alertSuccess(
                        $.t('ovs:users.new.complete'),
                        $.t('ovs:users.new.addsuccess')
                    );
                })
                .fail(function(error) {
                    error = $.parseJSON(error.responseText);
                    generic.alertError(
                        $.t('ovs:generic.error'),
                        $.t('ovs:users.new.addfailed', { why: error.detail })
                    );
                })
                .always(function() {
                    self.newUser(self.buildUser());
                });
        };
        self.deleteUser = function(guid) {
            $.each(self.users(), function(i, user) {
                if (user.guid() === guid) {
                    app.showMessage(
                        $.t('ovs:users.delete.delete', { what: user.username() }),
                        $.t('ovs:generic.areyousure'),
                        [$.t('ovs:generic.no'), $.t('ovs:generic.yes')]
                    )
                    .done(function(answer) {
                        if (answer === $.t('ovs:generic.yes')) {
                            api.del('users/' + guid)
                                .done(function () {
                                    generic.alertSuccess(
                                        $.t('ovs:users.delete.complete'),
                                        $.t('ovs:users.delete.deletesuccess')
                                    );
                                    self.users.remove(user);
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

        // More observables
        self.newUser   = ko.observable(self.buildUser());
        self.newClient = ko.observable(self.buildClient());

        // Durandal
        self.activate = function() {
            self.refresher.init(function() {
                self.fetchGroups();
                self.fetchRoles();
            }, 5000);
            self.refresher.start();
            self.refresher.run();
        };
        self.deactivate = function() {
            $.each(self.widgets, function(i, item) {
                item.deactivate();
            });
            self.refresher.stop();
        };
    };
});
