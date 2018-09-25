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
    'ovs/api', 'ovs/shared', 'ovs/generic', 'ovs/plugins/pluginloader',
    './data', './constants',
    'viewmodels/containers/backend/backend', 'viewmodels/containers/backend/backendtype',
    'viewmodels/services/backend'
], function($, ko, router,
            api, shared, generic, pluginLoader,
            data, Constants,
            Backend, BackendType,
            backendService) {
    "use strict";
    return function(stepOptions) {
        var self = this;

        // Variables
        self.pluginViews = [];
        self.data   = stepOptions.data;
        self.shared = shared;

        // Handles
        self.loadBackendsHandle     = undefined;
        self.loadBackendTypesHandle = undefined;

        // Computed
        self.modules = ko.pureComputed(function() {
            if (self.data.selectedBackendType() === undefined) {
                return [];
            }
            return self.pluginViews;
        });
        self.canContinue = ko.pureComputed(function() {
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
            var postData = {
                name: self.data.name(),
                backend_type_guid: self.data.selectedBackendType().guid()
            };
            var chain = backendService.addBackend(postData);
            $.each(self.modules(), function(index, module) {
                chain.then(module.module.finish);
            });
            chain.then(
                function() {
                    generic.alertInfo(
                        $.t('ovs:wizards.add_backend.gather.creating'),
                        $.t('ovs:wizards.add_backend.gather.started')
                    )
                }, function(error) {
                    error = generic.extractErrorMessage(error);
                    generic.alertError(
                        $.t('ovs:generic.error'),
                        $.t('ovs:wizards.add_backend.gather.failed', {why: error})
                    );
            });
            return chain
        };

        // Durandal
        self.activate = function() {
            self.pluginViews = pluginLoader.get_plugin_wizards(Constants.wizard_identifier);
            $.each(self.pluginViews, function(index, view){
                pluginLoader.activate_page(view)
            });
            if (generic.xhrCompleted(self.loadBackendsHandle)) {
                var options = {
                    sort: 'name',
                    contents: ''
                };
                self.loadBackendsHandle = backendService.loadBackends(options)
                    .then(function (data) {
                        self.data.update({backends: data.data});
                    });
            }
            return $.when()
                .then(function() {
                    if (generic.xhrCompleted(self.loadBackendTypesHandle)) {
                        var options = {
                            sort: 'name',
                            contents: '',
                            query: JSON.stringify({
                                type: 'AND',
                                items: [['has_plugin', 'EQUALS', true]]
                            })
                        };
                        return self.loadBackendTypesHandle = backendService.loadBackendTypes(options)
                            .then(function(data) {
                                self.data.update({backendTypes: data.data});
                            })
                    }
                })
        };
        self.deactivate = function() {
            $.each(self.pluginViews, function(index, view){
                pluginLoader.deactivate_page(view)
            });
        };
    };
});
