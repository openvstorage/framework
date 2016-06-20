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
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/api',
    '../containers/vmachine', '../containers/vpool', '../wizards/createfromtemplatewizard/index'
], function($, app, dialog, ko, shared, generic, api, VMachine, VPool, CreateFromTemplateWizard) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared           = shared;
        self.guard            = { authenticated: true };
        self.widgets          = [];
        self.query            = {
            type: 'AND',
            items: [['is_vtemplate', 'EQUALS', true]]
        };
        self.vTemplateHeaders = [
            { key: 'name',     value: $.t('ovs:generic.name'),     width: undefined },
            { key: undefined,  value: $.t('ovs:generic.vdisks'),   width: 60        },
            { key: 'children', value: $.t('ovs:generic.children'), width: 110       },
            { key: undefined,  value: $.t('ovs:generic.actions'),  width: 80        }
        ];

        // Handles
        self.vTemplatesHandle = {};
        self.vPoolsHandle     = undefined;

        // Observables
        self.vTemplates = ko.observableArray([]);

        // Functions
        self.loadVTemplates = function(options) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.vTemplatesHandle[options.page])) {
                    options.sort = 'name';
                    options.contents = 'vdisks';
                    options.query = JSON.stringify(self.query);
                    self.vTemplatesHandle[options.page] = api.get('vmachines', { queryparams: options })
                        .done(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    var vm = new VMachine(guid);
                                    self.vTemplates.push(vm);
                                    return vm;
                                },
                                dependencyLoader: function(item) {
                                    item.fetchTemplateChildrenGuids();
                                }
                            });
                        })
                        .fail(function() { deferred.reject(); });
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.deleteVT = function(guid) {
            $.each(self.vTemplates(), function(index, vm) {
                if (vm.guid() === guid && vm.templateChildrenGuids().length === 0) {
                    app.showMessage(
                            $.t('ovs:vmachines.delete.warning', { what: vm.name() }),
                            $.t('ovs:generic.areyousure'),
                            [$.t('ovs:generic.no'), $.t('ovs:generic.yes')]
                        )
                        .done(function(answer) {
                            if (answer === $.t('ovs:generic.yes')) {
                                self.vTemplates.destroy(vm);
                                generic.alertInfo(
                                    $.t('ovs:vmachines.delete.marked'),
                                    $.t('ovs:vmachines.delete.markedmsg', { what: vm.name() })
                                );
                                api.post('vmachines/' + vm.guid() + '/delete_vtemplate')
                                    .then(self.shared.tasks.wait)
                                    .done(function() {
                                        generic.alertSuccess(
                                            $.t('ovs:vmachines.delete.done'),
                                            $.t('ovs:vmachines.delete.donemsg', { what: vm.name() })
                                        );
                                    })
                                    .fail(function(error) {
                                        generic.alertError(
                                            $.t('ovs:generic.error'),
                                            $.t('ovs:generic.messages.errorwhile', {
                                                context: 'error',
                                                what: $.t('ovs:vmachines.delete.errormsg', { what: vm.name() }),
                                                error: error.responseText
                                            })
                                        );
                                    });
                            }
                        });
                }
            });
        };
        self.createFromTemplate = function(guid) {
            dialog.show(new CreateFromTemplateWizard({
                modal: true,
                vmachineguid: guid
            }));
        };

        // Durandal
        self.deactivate = function() {
            $.each(self.widgets, function(index, item) {
                item.deactivate();
            });
        };
    };
});
