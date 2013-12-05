// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vmachine'
], function($, app, dialog, ko, shared, generic, Refresher, api, VMachine) {
    "use strict";
    return function() {
        var self = this;

        // System
        self.shared    = shared;
        self.guard     = { authenticated: true };
        self.refresher = new Refresher();
        self.widgets   = [];

        // Data
        self.vTemplateHeaders = [
            { key: 'name',         value: $.t('ovs:generic.name'),       width: undefined, colspan: undefined },
            { key: undefined,      value: $.t('ovs:generic.disks'),      width: 60,        colspan: undefined },
            { key: 'children',     value: $.t('ovs:generic.children'),   width: 110,       colspan: undefined },
            { key: undefined,      value: $.t('ovs:generic.actions'),    width: 80,        colspan: undefined }
        ];
        self.vTemplates = ko.observableArray([]);
        self.vTemplateGuids         =  [];
        self.vTemplateChildrenGuids =  [];

        // Variables
        self.loadVTemplatesHandle = undefined;

        // Functions
        self.load = function() {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.loadVTemplatesHandle);
                var query = {
                        query: {
                            type: 'AND',
                            items: [['is_internal', 'EQUALS', false],
                                    ['is_vtemplate', 'EQUALS', true]]
                        }
                    };
                self.loadVTemplatesHandle = api.post('vmachines/filter', query)
                    .done(function(data) {
                        var i, guids = [];
                        for (i = 0; i < data.length; i += 1) {
                            guids.push(data[i].guid);
                        }
                        generic.crossFiller(
                            guids, self.vTemplateGuids, self.vTemplates,
                            function(guid) {
                                return new VMachine(guid);
                            }
                        );
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };
        self.loadVTemplate = function(vt) {
            vt.load(),
            vt.fetchTemplateChildrenGuids()
        };
        self.deleteVT = function(guid) {
            var i, vts = self.vTemplates(), vm;
            for (i = 0; i < vts.length; i += 1) {
                if (vts[i].guid() === guid) {
                    vm = vts[i];
                }
            }
            if (vm !== undefined) {
                (function(vm) {
                    app.showMessage(
                            $.t('ovs:vmachines.delete.warning', { what: vm.name() }),
                            $.t('ovs:generic.areyousure'),
                            [$.t('ovs:generic.yes'), $.t('ovs:generic.no')]
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
                                            $.t('ovs:generic.errorwhile', {
                                                context: 'error',
                                                what: $.t('ovs:vmachines.delete.errormsg', { what: vm.name() }),
                                                error: error
                                            })
                                        );
                                    });
                            }
                        });
                }(vm));
            }
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(self.load, 5000);
            self.refresher.run();
            self.refresher.start();
            self.shared.footerData(self.vTemplates);
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
