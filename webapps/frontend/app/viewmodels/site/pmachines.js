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
    '../containers/pmachine', '../containers/mgmtcenter',
    '../wizards/addmgmtcenter/index'
], function($, app, dialog, ko, shared, generic, Refresher, api, PMachine, MgmtCenter, AddMgmtCenterWizard) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared            = shared;
        self.guard             = { authenticated: true };
        self.refresher         = new Refresher();
        self.widgets           = [];
        self.mgmtCenterHeaders = [
            { key: 'name',      value: $.t('ovs:generic.name'),               width: 250       },
            { key: 'ipAddress', value: $.t('ovs:generic.ip'),                 width: 150       },
            { key: 'port',      value: $.t('ovs:generic.port'),               width: 60        },
            { key: 'type',      value: $.t('ovs:generic.type'),               width: 150       },
            { key: 'username',  value: $.t('ovs:generic.username'),           width: 150       },
            { key: undefined,   value: $.t('ovs:pmachines.mgmtcenter.hosts'), width: undefined },
            { key: undefined,   value: $.t('ovs:generic.actions'),            width: 60        }
        ];
        self.pMachineHeaders   = [
            { key: 'name',            value: $.t('ovs:generic.name'),       width: 250       },
            { key: 'ipAddress',       value: $.t('ovs:generic.ip'),         width: 150       },
            { key: 'hvtype',          value: $.t('ovs:generic.type'),       width: 150       },
            { key: 'mgmtcenter_guid', value: $.t('ovs:generic.mgmtcenter'), width: undefined }
        ];
        self.mgmtCenterMapping = {};

        // Observables
        self.pMachines              = ko.observableArray([]);
        self.mgmtCenters            = ko.observableArray([]);
        self.pMachinesInitialLoad   = ko.observable(true);
        self.mgmtCentersInitialLoad = ko.observable(true);

        // Handles
        self.loadPMachinesHandle      = undefined;
        self.loadMgmtCentersHandle    = undefined;

        // Functions
        self.loadPMachines = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadPMachinesHandle)) {
                    var options = {
                        sort: 'name',
                        contents: 'mgmtcenter'
                    };
                    if (page !== undefined) {
                        options.page = page;
                    }
                    self.loadPMachinesHandle = api.get('pmachines', undefined, options)
                        .done(function(data) {
                            var guids = [], pmdata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                pmdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.pMachines,
                                function(guid) {
                                    return new PMachine(guid);
                                }, 'guid'
                            );
                            $.each(self.pMachines(), function(index, pmachine) {
                                if ($.inArray(pmachine.guid(), guids) !== -1) {
                                    pmachine.fillData(pmdata[pmachine.guid()]);
                                }
                            });
                            self.pMachinesInitialLoad(false);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.loadMgmtCenters = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadMgmtCentersHandle)) {
                    var options = {
                        sort: 'name',
                        contents: ''
                    };
                    if (page !== undefined) {
                        options.page = page;
                    }
                    self.loadMgmtCentersHandle = api.get('mgmtcenters', undefined, options)
                        .done(function(data) {
                            var guids = [], mcdata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                mcdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.mgmtCenters,
                                function(guid) {
                                    var mc = new MgmtCenter(guid);
                                    if (!self.mgmtCenterMapping.hasOwnProperty(guid)) {
                                        self.mgmtCenterMapping[guid] = mc;
                                    }
                                    return mc;
                                }, 'guid'
                            );
                            $.each(self.mgmtCenters(), function(index, mgmtCenter) {
                                if ($.inArray(mgmtCenter.guid(), guids) !== -1) {
                                    mgmtCenter.fillData(mcdata[mgmtCenter.guid()]);
                                }
                            });
                            self.mgmtCentersInitialLoad(false);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.addMgmtCenter = function() {
            dialog.show(new AddMgmtCenterWizard({
                modal: true
            }));
        };
        self.deleteMgmtCenter = function(guid) {
            var mgmtCenter;
            $.each(self.mgmtCenters(), function(index, mc) {
                if (mc.guid() === guid) {
                    mgmtCenter = mc;
                }
            });
            if (mgmtCenter !== undefined) {
                app.showMessage(
                        $.t('ovs:pmachines.delete.warning', { what: mgmtCenter.name() }),
                        $.t('ovs:generic.areyousure'),
                        [$.t('ovs:generic.no'), $.t('ovs:generic.yes')]
                    )
                    .done(function(answer) {
                        if (answer === $.t('ovs:generic.yes')) {
                            self.mgmtCenters.destroy(mgmtCenter);
                            generic.alertInfo(
                                $.t('ovs:pmachines.delete.marked'),
                                $.t('ovs:pmachines.delete.markedmsg', { what: mgmtCenter.name() })
                            );
                            api.del('mgmtcenters/' + mgmtCenter.guid())
                                .done(function() {
                                    generic.alertSuccess(
                                        $.t('ovs:pmachines.delete.done'),
                                        $.t('ovs:pmachines.delete.donemsg', { what: mgmtCenter.name() })
                                    );
                                })
                                .fail(function(error) {
                                    generic.alertError(
                                        $.t('ovs:generic.error'),
                                        $.t('ovs:generic.messages.errorwhile', {
                                            context: 'error',
                                            what: $.t('ovs:pmachines.delete.errormsg', { what: mgmtCenter.name() }),
                                            error: error
                                        })
                                    );
                                });
                        }
                    });
            }
        };

        // Durandal
        self.activate = function() {
            self.loadPMachines();
            self.loadMgmtCenters();
        };
    };
});
