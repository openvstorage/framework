// license see http://www.openvstorage.com/licenses/opensource/
/*global define, window */
define([
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vmachine', '../containers/pmachine'
], function($, app, dialog, ko, shared, generic, Refresher, api, VMachine, PMachine) {
    "use strict";
    return function() {
        var self = this;

        // System
        self.shared      = shared;
        self.guard       = { authenticated: true };
        self.refresher   = new Refresher();
        self.widgets     = [];
        self.sortTimeout = undefined;

        // Data
        self.vsasHeaders = [
            { key: 'status',     value: $.t('ovs:generic.status'),     width: 55,  colspan: undefined },
            { key: 'name',       value: $.t('ovs:generic.name'),       width: 100, colspan: undefined },
            { key: 'ip',         value: $.t('ovs:generic.ip'),         width: 100, colspan: undefined },
            { key: 'host',       value: $.t('ovs:generic.host'),       width: 55,  colspan: undefined },
            { key: 'type',       value: $.t('ovs:generic.type'),       width: 55,  colspan: undefined },
            { key: 'vdisks',     value: $.t('ovs:generic.vdisks'),     width: 55,  colspan: undefined },
            { key: 'storedData', value: $.t('ovs:generic.storeddata'), width: 100, colspan: undefined },
            { key: 'cacheRatio', value: $.t('ovs:generic.cache'),      width: 100, colspan: undefined },
            { key: 'iops',       value: $.t('ovs:generic.iops'),       width: 55,  colspan: undefined },
            { key: 'readSpeed',  value: $.t('ovs:generic.read'),       width: 100, colspan: undefined },
            { key: 'writeSpeed', value: $.t('ovs:generic.write'),      width: 100, colspan: undefined }
        ];
        self.vsas = ko.observableArray([]);
        self.vsaGuids = [];

        // Variables
        self.loadVsasHandle = undefined;
        self.pMachineCache = {};

        // Functions
        self.load = function() {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.loadVsasHandle);
                var query = {
                    query: {
                        type: 'AND',
                        items: [['is_internal', 'EQUALS', true]]
                    }
                };
                self.loadVsasHandle = api.post('vmachines/filter', query)
                    .done(function(data) {
                        var i, guids = [];
                        for (i = 0; i < data.length; i += 1) {
                            guids.push(data[i].guid);
                        }
                        generic.crossFiller(
                            guids, self.vsaGuids, self.vsas,
                            function(guid) {
                                return new VMachine(guid);
                            }, 'guid'
                        );
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };
        self.loadVSA = function(vsa) {
            return $.Deferred(function(deferred) {
                vsa.load()
                    .done(function() {
                        var pMachineGuid = vsa.pMachineGuid(), pm;
                        if (pMachineGuid && (vsa.pMachine() === undefined || vsa.pMachine().guid() !== pMachineGuid)) {
                            if (!self.pMachineCache.hasOwnProperty(pMachineGuid)) {
                                pm = new PMachine(pMachineGuid);
                                pm.load();
                                self.pMachineCache[pMachineGuid] = pm;
                            }
                            vsa.pMachine(self.pMachineCache[pMachineGuid]);
                        }
                        // (Re)sort VSAs
                        if (self.sortTimeout) {
                            window.clearTimeout(self.sortTimeout);
                        }
                        self.sortTimeout = window.setTimeout(function() { generic.advancedSort(self.vsas, ['name', 'guid']); }, 250);
                    })
                    .always(deferred.resolve);
            }).promise();
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(self.load, 5000);
            self.refresher.start();
            self.shared.footerData(self.vsas);

            self.load()
                .done(function() {
                    var i, vsas = self.vsas();
                    for (i = 0; i < vsas.length; i += 1) {
                        self.loadVSA(vsas[i]);
                    }
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
