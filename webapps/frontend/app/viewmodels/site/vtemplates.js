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
    '../containers/vdisk', '../wizards/createfromtemplate/index'
], function($, app, dialog, ko, shared, generic, Refresher, api, VDisk, CreateFromTemplate) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared                  = shared;
        self.guard                   = { authenticated: true };
        self.refresher               = new Refresher();
        self.widgets                 = [];
        self.query                   = {
            type: 'AND',
            items: [['is_vtemplate', 'EQUALS', true]]
        };
        self.vDiskTemplateHeaders    = [
            { key: 'name',     value: $.t('ovs:generic.name'),     width: undefined },
            { key: 'children', value: $.t('ovs:generic.children'), width: 110       },
            { key: undefined,  value: $.t('ovs:generic.actions'),  width: 80        }
        ];

        // Observables
        self.vDiskTemplates    = ko.observableArray([]);

        // Handles
        self.vDiskTemplatesHandle    = {};

        // Functions
        self.loadVDiskTemplates = function(options) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.vDiskTemplatesHandle[options.page])) {
                    options.sort = 'name';
                    options.contents = '';
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
                                        error = generic.extractErrorMessage(error);
                                        generic.alertError(
                                            $.t('ovs:generic.error'),
                                            $.t('ovs:generic.messages.errorwhile', {
                                                context: 'error',
                                                what: $.t('ovs:vdisks.delete.error_msg', { what: vd.name() }),
                                                error: error
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
                guid: guid
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
