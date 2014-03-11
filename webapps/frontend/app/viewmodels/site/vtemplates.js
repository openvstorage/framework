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
/*global define, window */
define([
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vmachine', '../wizards/createfromtemplatewizard/index'
], function($, app, dialog, ko, shared, generic, Refresher, api, VMachine, CreateFromTemplateWizard) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared           = shared;
        self.guard            = { authenticated: true };
        self.refresher        = new Refresher();
        self.widgets          = [];
        self.query            = {
            query: {
                type: 'AND',
                items: [['is_internal', 'EQUALS', false],
                        ['is_vtemplate', 'EQUALS', true]]
            }
        };
        self.vTemplateHeaders = [
            { key: 'name',         value: $.t('ovs:generic.name'),       width: undefined },
            { key: undefined,      value: $.t('ovs:generic.disks'),      width: 60        },
            { key: 'children',     value: $.t('ovs:generic.children'),   width: 110       },
            { key: undefined,      value: $.t('ovs:generic.actions'),    width: 80        }
        ];

        // Observables
        self.vTemplates            = ko.observableArray([]);
        self.vTemplatesInitialLoad = ko.observable(true);

        // Handles
        self.loadVTemplatesHandle    = undefined;
        self.refreshVTemplatesHandle = {};

        // Functions
        self.fetchVTemplates = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadVTemplatesHandle)) {
                    var options = {
                        sort: 'name',
                        full: true,
                        contents: ''
                    };
                    self.loadVTemplatesHandle = api.post('vmachines/filter', self.query, options)
                        .done(function(data) {
                            var guids = [], vtdata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                vtdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.vTemplates,
                                function(guid) {
                                    var vmachine = new VMachine(guid);
                                    if ($.inArray(guid, guids) !== -1) {
                                        vmachine.fillData(vtdata[guid]);
                                    }
                                    vmachine.loading(true);
                                    return vmachine;
                                }, 'guid'
                            );
                            self.vTemplatesInitialLoad(false);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.refreshVTemplates = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.refreshVTemplatesHandle[page])) {
                    var options = {
                        sort: 'name',
                        full: true,
                        page: page,
                        contents: ''
                    };
                    self.refreshVTemplatesHandle[page] = api.post('vmachines/filter', self.query, options)
                        .done(function(data) {
                            var guids = [], vtdata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                vtdata[item.guid] = item;
                            });
                            $.each(self.vTemplates(), function(index, vt) {
                                if ($.inArray(vt.guid(), guids) !== -1) {
                                    vt.fillData(vtdata[vt.guid()]);
                                    vt.fetchTemplateChildrenGuids();
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
        self.createFromTemplate = function(guid) {
            dialog.show(new CreateFromTemplateWizard({
                modal: true,
                vmachineguid: guid
            }));
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(self.fetchVTemplates, 5000);
            self.refresher.start();
            self.shared.footerData(self.vTemplates);

            self.fetchVTemplates().then(function() {
                self.refreshVTemplates(1);
            });
        };
        self.deactivate = function() {
            $.each(self.widgets, function(index, item) {
                item.deactivate();
            });
            self.refresher.stop();
            self.shared.footerData(ko.observable());
        };
    };
});
