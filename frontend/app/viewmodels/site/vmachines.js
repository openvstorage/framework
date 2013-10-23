define([
    'durandal/app', 'plugins/dialog',
    'ovs/shared', 'knockout', 'ovs/generic', 'ovs/authentication', 'ovs/refresher',
    '../containers/vmachine', '../wizards/clone/index'
], function (app, dialog, shared, ko, generic, authentication, Refresher, VMachine, CloneWizard) {
    "use strict";
    return function () {
        var self = this;

        // System
        self.shared = shared;
        self.refresher = new Refresher();

        // Data
        self.displayname = 'vMachines';
        self.description = 'This page contains a first overview of the vmachines and their vdisks in our model';

        self.vmachine_headers = [
            { key: 'name',    value: 'Name',   width: 300 },
            { key: undefined, value: 'Disks',  width: undefined },
            { key: undefined, value: '&nbsp;', width: 35 },
            { key: undefined, value: '&nbsp;', width: 35 }
        ];
        self.vmachines = ko.observableArray([]);
        self.vmachine_guids =  [];

        // Variables
        self.load_vmachines_handle = undefined;

        // Functions
        self.load = function() {
            return $.Deferred(function (deferred) {
                if (self.load_vmachines_handle !== undefined) {
                    self.load_vmachines_handle.abort();
                }
                self.load_vmachines_handle = $.ajax('/api/internal/vmachines/?timestamp=' + generic.gettimestamp(), {
                    type: 'get',
                    contentType: 'application/json',
                    headers: {
                        'Authorization': authentication.header()
                    }
                })
                .done(function (data) {
                    var i, item, guids = [];
                    for (i = 0; i < data.length; i += 1) {
                        guids.push(data[i].guid);
                    }
                    for (i = 0; i < guids.length; i += 1) {
                        if ($.inArray(guids[i], self.vmachine_guids) === -1) {
                            self.vmachine_guids.push(guids[i]);
                            self.vmachines.push(new VMachine(guids[i]));
                        }
                    }
                    for (i = 0; i < self.vmachine_guids.length; i += 1) {
                        if ($.inArray(self.vmachine_guids[i], guids) === -1) {
                            self.vmachine_guids.splice(i, 1);
                            self.vmachines.splice(i, 1);
                        }
                    }
                    deferred.resolve();
                })
                .fail(deferred.reject);
            }).promise();
        };
        self.clone = function(guid) {
            var i, vms = self.vmachines();
            for (i = 0; i < vms.length; i += 1) {
                if (vms[i].guid() === guid) {
                    dialog.show(new CloneWizard({
                        modal: true,
                        machineguid: guid
                    }));
                }
            }
        };
        self.deletevm = function(guid) {
            var i, vms = self.vmachines();
            for (i = 0; i < vms.length; i += 1) {
                (function(i) {
                    if (vms[i].guid() === guid) {
                        app.showMessage('Are you sure you want to delete "' + vms[i].name() + '"?', 'Are you sure?', ['Yes', 'No'])
                            .done(function (answer) {
                                if (answer === 'Yes') {
                                    $.ajax('/api/internal/vmachines/' + vms[i].guid() + '/?timestamp=' + generic.gettimestamp(), {
                                        type: 'DELETE',
                                        contentType: 'application/json',
                                        data: JSON.stringify({}),
                                        headers: {
                                            'Authorization': authentication.header(),
                                            'X-CSRFToken': generic.get_cookie('csrftoken')
                                        }
                                    })
                                        .done(function (data) {
                                            generic.alert_success('Machine ' + vms[i].name() + ' deleted.');
                                        })
                                        .fail(function (xmlHttpRequest) {
                                            // We check whether we actually received an error, and it's not the browser navigating away
                                            if (xmlHttpRequest.readyState !== 0 && xmlHttpRequest.status !== 0) {
                                                generic.alert_error('Machine ' + vms[i].name() + ' deletion failed.');
                                            }
                                        });
                                }
                            });
                    }
                }(i));
            }
        };

        // Durandal
        self.canActivate = function() { return authentication.validate(); };
        self.activate = function () {
            self.refresher.init('vmachine', self.load, 5000);
            app.trigger('vmachine.refresher:run');
            app.trigger('vmachine.refresher:start');
        };
        self.deactivate = function () {
            app.trigger('vmachine.refresher:stop');
            self.refresher.destroy();
        };
    };
});