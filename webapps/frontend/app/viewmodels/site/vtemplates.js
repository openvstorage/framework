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

        // System
        self.shared      = shared;
        self.guard       = { authenticated: true };
        self.refresher   = new Refresher();
        self.widgets     = [];
        self.updateSort  = false;
        self.sortTimeout = undefined;

        // Data
        self.vTemplateHeaders = [
            { key: 'name',         value: $.t('ovs:generic.name'),       width: undefined },
            { key: undefined,      value: $.t('ovs:generic.disks'),      width: 60        },
            { key: 'children',     value: $.t('ovs:generic.children'),   width: 110       },
            { key: undefined,      value: $.t('ovs:generic.actions'),    width: 80        }
        ];
        self.vTemplates = ko.observableArray([]);
        self.vTemplatesInitialLoad = ko.observable(true);

        // Variables
        self.loadVTemplatesHandle = undefined;

        // Functions
        self.load = function(full) {
            full = full || false;
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadVTemplatesHandle)) {
                    var query = {
                            query: {
                                type: 'AND',
                                items: [['is_internal', 'EQUALS', false],
                                        ['is_vtemplate', 'EQUALS', true]]
                            }
                        }, filter = {};
                    if (full) {
                        filter.full = true;
                    }
                    self.loadVTemplatesHandle = api.post('vmachines/filter', query, filter)
                        .done(function(data) {
                            var i, guids = [], vmdata = {};
                            for (i = 0; i < data.length; i += 1) {
                                guids.push(data[i].guid);
                                vmdata[data[i].guid] = data[i];
                            }
                            generic.crossFiller(
                                guids, self.vTemplates,
                                function(guid) {
                                    var vm = new VMachine(guid);
                                    if (full) {
                                        vm.fillData(vmdata[guid]);
                                    }
                                    return vm;
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
        self.loadVTemplate = function(vt) {
            return $.Deferred(function(deferred) {
                vt.load()
                    .then(vt.fetchTemplateChildrenGuids)
                    .done(function() {
                        // (Re)sort vTemplates
                        if (self.updateSort) {
                            self.sort();
                        }
                    })
                    .always(deferred.resolve);
            }).promise();
        };
        self.sort = function() {
            if (self.sortTimeout) {
                window.clearTimeout(self.sortTimeout);
            }
            self.sortTimeout = window.setTimeout(function() { generic.advancedSort(self.vTemplates, ['name', 'guid']); }, 250);
        };
        self.deleteVT = function(guid) {
            var i, vts = self.vTemplates(), vm;
            for (i = 0; i < vts.length; i += 1) {
                if (vts[i].guid() === guid) {
                    vm = vts[i];
                }
            }
            if (vm !== undefined) {
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
                            api.del('vmachines/' + vm.guid())
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
                                            error: error
                                        })
                                    );
                                });
                        }
                    });
            }
        };
        self.createFromTemplate = function(guid) {
            dialog.show(new CreateFromTemplateWizard({
                modal: true,
                vmachineguid: guid
            }));
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(self.load, 5000);
            self.shared.footerData(self.vTemplates);

            self.load(true)
                .always(function() {
                    self.sort();
                    self.updateSort = true;
                    self.refresher.start();
                });
        };
        self.deactivate = function() {
            var i;
            for (i = 0; i < self.widgets.length; i += 2) {
                self.widgets[i].deactivate();
            }
            self.refresher.stop();
            self.shared.footerData(ko.observable());
        };
    };
});
