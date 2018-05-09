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
    'plugins/router',
    'ovs/api', 'ovs/shared', 'ovs/generic',
    './data',
    'viewmodels/containers/backend/backend', 'viewmodels/containers/backend/backendtype'
], function($, ko, router, api, shared, generic, data, Backend, BackendType) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data   = data;
        self.shared = shared;

        // Handles
        self.loadBackendsHandle     = undefined;
        self.loadBackendTypesHandle = undefined;

        // Computed
        self.modules = ko.computed(function() {
            var modules = [];
            if (self.data.backendType() === undefined) {
                return modules;
            }
            $.each(shared.hooks.wizards, function (wizard, wModules) {
                if (wizard === 'addbackend') {
                    $.each(wModules, function (index, module) {
                        if (module.name.toLowerCase() == self.data.backendType().name().toLowerCase()) {
                            modules.push(module);
                        }
                    });
                }
            });
            return modules;
        });
        self.canContinue = ko.computed(function() {
            var valid = true, reasons = [], fields = [];
            if (!self.data.name.valid()) {
                valid = false;
                fields.push('name');
                reasons.push($.t('ovs:wizards.add_backend.gather.invalid_name'));
            }
            $.each(self.data.backends(), function(index, backend) {
                if (backend.name() === self.data.name() && !fields.contains('name')) {
                    valid = false;
                    fields.push('name');
                    reasons.push($.t('ovs:wizards.add_backend.gather.duplicate_name'));
                }
            });
            $.each(self.modules(), function(index, module) {
                var cc = module.module.canContinue();
                if (cc.value === false) {
                    valid = false;
                    reasons = reasons.concat(cc.reasons);
                    fields = fields.concat(cc.fields);
                }
            });
            return { value: valid, reasons: reasons, fields: fields };
        });

        // Functions
        self.finish = function() {
            return $.Deferred(function(deferred) {
                var postData = {
                    data: {
                        name: self.data.name(),
                        backend_type_guid: self.data.backendType().guid()
                    }
                };
                var chain = api.post('backends', postData);
                $.each(self.modules(), function(index, module) {
                    chain.then(module.module.finish);
                });
                chain.done(function() {
                        generic.alertInfo(
                            $.t('ovs:wizards.add_backend.gather.creating'),
                            $.t('ovs:wizards.add_backend.gather.started')
                        );
                    })
                    .fail(function() {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.add_backend.gather.failed')
                        );
                    })
                    .always(deferred.resolve);
            }).promise();
        };

        // Durandal
        self.activate = function() {
            $.each(shared.hooks.wizards, function (wizard, modules) {
                $.each(modules, function (index, module) {
                    module.activator.activateItem(module.module);
                });
            });
            if (generic.xhrCompleted(self.loadBackendsHandle)) {
                var options = {
                    sort: 'name',
                    contents: ''
                };
                self.loadBackendsHandle = api.get('backends', { queryparams: options })
                    .done(function (data) {
                        var guids = [], bdata = {};
                        $.each(data.data, function (index, item) {
                            guids.push(item.guid);
                            bdata[item.guid] = item;
                        });
                        generic.crossFiller(
                            guids, self.data.backends,
                            function (guid) {
                                return new Backend(guid);
                            }, 'guid'
                        );
                        $.each(self.data.backends(), function (index, backend) {
                            if (guids.contains(backend.guid())) {
                                backend.fillData(bdata[backend.guid()]);
                            }
                        });
                    });
            }
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadBackendTypesHandle)) {
                    var options = {
                        sort: 'name',
                        contents: '',
                        query: JSON.stringify({
                            type: 'AND',
                            items: [['has_plugin', 'EQUALS', true]]
                        })
                    };
                    self.loadBackendTypesHandle = api.get('backendtypes', { queryparams: options })
                        .done(function(data) {
                            var guids = [], btdata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                btdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.data.backendTypes,
                                function(guid) {
                                    return new BackendType(guid);
                                }, 'guid'
                            );
                            $.each(self.data.backendTypes(), function(index, backendType) {
                                if ($.inArray(backendType.guid(), guids) !== -1) {
                                    backendType.fillData(btdata[backendType.guid()]);
                                }
                            });
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.deactivate = function() {
            $.each(shared.hooks.wizards, function (wizard, modules) {
                $.each(modules, function (index, module) {
                    module.activator.deactivateItem(module.module);
                });
            });
        };
    };
});
