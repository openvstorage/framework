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
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vmachine', '../containers/vdisk', '../wizards/createfromtemplate/index'
], function($, app, dialog, ko, shared, generic, Refresher, api, VMachine, VDisk, CreateFromTemplate) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared                  = shared;
        self.guard                   = { authenticated: true };
        self.refresher               = new Refresher();
        self.widgets                 = [];
        self.vMachineCache           = {};
        self.query                   = {
            type: 'AND',
            items: [['is_vtemplate', 'EQUALS', true]]
        };
        self.vMachineTemplateHeaders = [
            { key: 'name',     value: $.t('ovs:generic.name'),     width: undefined },
            { key: undefined,  value: $.t('ovs:generic.vdisks'),   width: 100       },
            { key: 'children', value: $.t('ovs:generic.children'), width: 110       },
            { key: undefined,  value: $.t('ovs:generic.actions'),  width: 80        }
        ];
        self.vDiskTemplateHeaders    = [
            { key: 'name',     value: $.t('ovs:generic.name'),     width: 300       },
            { key: 'vmachine', value: $.t('ovs:generic.vmachine'), width: undefined },
            { key: 'children', value: $.t('ovs:generic.children'), width: 110       },
            { key: undefined,  value: $.t('ovs:generic.actions'),  width: 80        }
        ];

        // Observables
        self.vMachineTemplates = ko.observableArray([]);
        self.vDiskTemplates    = ko.observableArray([]);
        self.vMachines         = ko.observableArray([]);

        // Handles
        self.vMachineTemplatesHandle = {};
        self.vDiskTemplatesHandle    = {};
        self.vMachineHandle          = undefined;

        // Functions
        self.loadVMachineTemplates = function(options) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.vMachineTemplatesHandle[options.page])) {
                    options.sort = 'name';
                    options.contents = 'vdisks';
                    options.query = JSON.stringify(self.query);
                    self.vMachineTemplatesHandle[options.page] = api.get('vmachines', { queryparams: options })
                        .done(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    return new VMachine(guid);
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
        self.deleteVMT = function(guid) {
            $.each(self.vMachineTemplates(), function(index, vm) {
                if (vm.guid() === guid && vm.templateChildrenGuids().length === 0) {
                    app.showMessage(
                            $.t('ovs:vmachines.delete.warning', { what: vm.name() }),
                            $.t('ovs:generic.areyousure'),
                            [$.t('ovs:generic.no'), $.t('ovs:generic.yes')]
                        )
                        .done(function(answer) {
                            if (answer === $.t('ovs:generic.yes')) {
                                generic.alertInfo(
                                    $.t('ovs:vmachines.delete.marked'),
                                    $.t('ovs:vmachines.delete.marked_msg', { what: vm.name() })
                                );
                                api.post('vmachines/' + vm.guid() + '/delete_vtemplate')
                                    .then(self.shared.tasks.wait)
                                    .done(function() {
                                        generic.alertSuccess(
                                            $.t('ovs:vmachines.delete.done'),
                                            $.t('ovs:vmachines.delete.done_msg', { what: vm.name() })
                                        );
                                    })
                                    .fail(function(error) {
                                        generic.alertError(
                                            $.t('ovs:generic.error'),
                                            $.t('ovs:generic.messages.errorwhile', {
                                                context: 'error',
                                                what: $.t('ovs:vmachines.delete.error_msg', { what: vm.name() }),
                                                error: error.responseText
                                            })
                                        );
                                    });
                            }
                        });
                }
            });
        };
        self.createFromVMT = function(guid) {
            dialog.show(new CreateFromTemplate({
                modal: true,
                mode: 'vmachine',
                guid: guid
            }));
        };

        self.loadVDiskTemplates = function(options) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.vDiskTemplatesHandle[options.page])) {
                    options.sort = 'name';
                    options.contents = 'vmachine';
                    options.query = JSON.stringify(self.query);
                    self.vDiskTemplatesHandle[options.page] = api.get('vdisks', { queryparams: options })
                        .done(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    return new VDisk(guid);
                                },
                                dependencyLoader: function(item) {
                                    item.fetchTemplateChildrenGuids();
                                    var vm, vMachineGuid = item.vMachineGuid();
                                    if (vMachineGuid && (item.vMachine() === undefined || item.vMachine().guid() !== vMachineGuid)) {
                                        if (!self.vMachineCache.hasOwnProperty(vMachineGuid)) {
                                            vm = new VMachine(vMachineGuid);
                                            self.vMachineCache[vMachineGuid] = vm;
                                        }
                                        item.vMachine(self.vMachineCache[vMachineGuid]);
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
        self.deleteVDT = function(guid) {
            $.each(self.vDiskTemplates(), function(index, vd) {
                if (vd.guid() === guid && vd.templateChildrenGuids().length === 0) {
                    app.showMessage(
                            $.t('ovs:vdisks.delete.warning', { what: vd.name() }),
                            $.t('ovs:generic.areyousure'),
                            [$.t('ovs:generic.no'), $.t('ovs:generic.yes')]
                        )
                        .done(function(answer) {
                            if (answer === $.t('ovs:generic.yes')) {
                                generic.alertInfo(
                                    $.t('ovs:vdisks.delete.marked'),
                                    $.t('ovs:vdisks.delete.marked_msg', { what: vd.name() })
                                );
                                api.post('vdisks/' + vd.guid() + '/delete_vtemplate')
                                    .then(self.shared.tasks.wait)
                                    .done(function() {
                                        generic.alertSuccess(
                                            $.t('ovs:vdisks.delete.done'),
                                            $.t('ovs:vdisks.delete.done_msg', { what: vd.name() })
                                        );
                                    })
                                    .fail(function(error) {
                                        generic.alertError(
                                            $.t('ovs:generic.error'),
                                            $.t('ovs:generic.messages.errorwhile', {
                                                context: 'error',
                                                what: $.t('ovs:vdisks.delete.error_msg', { what: vd.name() }),
                                                error: error.responseText
                                            })
                                        );
                                    });
                            }
                        });
                }
            });
        };
        self.createFromVDT = function(guid) {
            dialog.show(new CreateFromTemplate({
                modal: true,
                mode: 'vdisk',
                guid: guid
            }));
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(function() {
                if (generic.xhrCompleted(self.vMachineHandle)) {
                    self.vMachineHandle = api.get('vmachines', { queryparams: { contents: '' }})
                        .done(function(data) {
                            var guids = [], vmdata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                vmdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.vMachines,
                                function(guid) {
                                    if (!self.vMachineCache.hasOwnProperty(guid)) {
                                        self.vMachineCache[guid] = new VMachine(guid);
                                    }
                                    return self.vMachineCache[guid];
                                }, 'guid'
                            );
                            $.each(self.vMachines(), function(index, item) {
                                if (vmdata.hasOwnProperty(item.guid())) {
                                    item.fillData(vmdata[item.guid()]);
                                }
                            });
                        });
                }
            }, 60000);
            self.refresher.start();
            self.refresher.run();
        };
        self.deactivate = function() {
            $.each(self.widgets, function(index, item) {
                item.deactivate();
            });
        };
    };
});
