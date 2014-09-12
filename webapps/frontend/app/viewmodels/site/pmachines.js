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
            { key: 'mgmtcenter_guid', value: $.t('ovs:generic.mgmtcenter'), width: undefined },
            { key: undefined,         value: '',                            width: 30        }
        ];

        // Observables
        self.pMachines              = ko.observableArray([]);
        self.mgmtCenters            = ko.observableArray([]);
        self.pMachinesInitialLoad   = ko.observable(true);
        self.mgmtCentersInitialLoad = ko.observable(true);
        self.mgmtCenterMapping      = ko.observable({});

        // Handles
        self.loadPMachinesHandle      = undefined;
        self.loadMgmtCentersHandle    = undefined;

        // Computed
        self.hostMapping = ko.computed(function() {
           var pMachineIPs = [], mapping = {}, match = false;
            $.each(self.pMachines(), function(pindex, pMachine) {
                pMachineIPs.push(pMachine.ipAddress());
            });
            $.each(self.mgmtCenters(), function(mindex, mgmtCenter) {
                mapping[mgmtCenter.guid()] = {
                    ovs: 0,
                    total: 0
                };
                $.each(mgmtCenter.hosts(), function(hindex, host) {
                    mapping[mgmtCenter.guid()].total += 1;
                    match = false;
                    $.each(host.ips, function(iindex, ip) {
                        if ($.inArray(ip, pMachineIPs) !== -1) {
                            match = true;
                            return false;
                        }
                        return true;
                    });
                    if (match) {
                        mapping[mgmtCenter.guid()].ovs += 1;
                    }
                });
            });
            return mapping;
        });
        self.mgmtCenterChoices = ko.computed(function() {
            var centers = self.mgmtCenters().slice();
            centers.push(undefined);
            return centers;
        });

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
                    self.loadPMachinesHandle = api.get('pmachines', { queryparams: options })
                        .done(function(data) {
                            var guids = [], pmdata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                pmdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.pMachines,
                                function(guid) {
                                    var pm = new PMachine(guid);
                                    pm.mgmtCenter = ko.computed({
                                        write: function(mgmtCenter) {
                                            if (mgmtCenter === undefined) {
                                                this.mgmtCenterGuid(undefined);
                                            } else {
                                                this.mgmtCenterGuid(mgmtCenter.guid());
                                            }
                                        },
                                        read: function() {
                                            if (self.mgmtCenterMapping().hasOwnProperty(this.mgmtCenterGuid())) {
                                                return self.mgmtCenterMapping()[this.mgmtCenterGuid()];
                                            }
                                            return undefined;
                                        },
                                        owner: pm
                                    });
                                    pm.mgmtCenterValid = ko.computed({
                                        read: function() {
                                            // Currently, matching is based on ip address
                                            // TODO: Replace this by hypervisorid matching
                                            var mgmtCenterGuid, pmachine = this,
                                                currentMgmtCenterGuid = pmachine.mgmtCenterGuid();
                                            $.each(self.mgmtCenters(), function(mcindex, mgmtCenter) {
                                                $.each(mgmtCenter.hosts(), function(hindex, host) {
                                                    $.each(host.ips, function(iindex, ip) {
                                                        if (ip === pmachine.ipAddress()) {
                                                            mgmtCenterGuid = mgmtCenter.guid();
                                                            return false;
                                                        }
                                                        return true;
                                                    });
                                                    return mgmtCenterGuid === undefined;
                                                });
                                                return mgmtCenterGuid === undefined;
                                            });
                                            if (mgmtCenterGuid === undefined && (currentMgmtCenterGuid === null || currentMgmtCenterGuid === undefined)) {
                                                return true;
                                            }
                                            return mgmtCenterGuid === pmachine.mgmtCenterGuid();
                                        },
                                        owner: pm
                                    });
                                    return pm;
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
                        contents: 'hosts'
                    };
                    if (page !== undefined) {
                        options.page = page;
                    }
                    self.loadMgmtCentersHandle = api.get('mgmtcenters', { queryparams: options })
                        .done(function(data) {
                            var guids = [], mcdata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                mcdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.mgmtCenters,
                                function(guid) {
                                    var mc = new MgmtCenter(guid),
                                        mapping = self.mgmtCenterMapping();

                                    if (!mapping.hasOwnProperty(guid)) {
                                        mapping[guid] = mc;
                                        self.mgmtCenterMapping(mapping);
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
                                            error: error.responseText
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
