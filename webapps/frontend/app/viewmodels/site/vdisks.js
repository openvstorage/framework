// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vdisk', '../containers/vmachine', '../containers/vpool', '../wizards/rollback/index'
], function($, app, dialog, ko, shared, generic, Refresher, api, VDisk, VMachine, VPool, RollbackWizard) {
    "use strict";
    return function() {
        var self = this;

        // System
        self.shared    = shared;
        self.guard     = { authenticated: true };
        self.refresher = new Refresher();
        self.widgets   = [];

        // Data
        self.vDiskHeaders = [
            { key: 'name',         value: $.t('ovs:generic.name'),         width: 150,       colspan: undefined },
            { key: 'vmachine',     value: $.t('ovs:generic.vmachine'),     width: 110,       colspan: undefined },
            { key: 'vpool',        value: $.t('ovs:generic.vpool'),        width: 110,       colspan: undefined },
            { key: 'vsa',          value: $.t('ovs:generic.vsa'),          width: 110,       colspan: undefined },
            { key: 'size',         value: $.t('ovs:generic.size'),         width: 100,       colspan: undefined },
            { key: 'storedData',   value: $.t('ovs:generic.storeddata'),   width: 110,       colspan: undefined },
            { key: 'cacheRatio',   value: $.t('ovs:generic.cache'),        width: 100,       colspan: undefined },
            { key: 'iops',         value: $.t('ovs:generic.iops'),         width: 55,        colspan: undefined },
            { key: 'readSpeed',    value: $.t('ovs:generic.read'),         width: 100,       colspan: undefined },
            { key: 'writeSpeed',   value: $.t('ovs:generic.write'),        width: 100,       colspan: undefined },
            { key: 'failoverMode', value: $.t('ovs:generic.focstatus'),    width: undefined, colspan: undefined },
            { key: undefined,      value: $.t('ovs:generic.actions'),      width: 80,        colspan: undefined }
        ];
        self.vDisks = ko.observableArray([]);
        self.vDiskGuids =  [];

        // Variables
        self.loadVDisksHandle = undefined;

        // Functions
        self.load = function() {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.loadVDisksHandle);
                self.loadVDisksHandle = api.get('vdisks')
                    .done(function(data) {
                        var i, guids = [];
                        for (i = 0; i < data.length; i += 1) {
                            guids.push(data[i].guid);
                        }
                        generic.crossFiller(
                            guids, self.vDiskGuids, self.vDisks,
                            function(guid) {
                                return new VDisk(guid);
                            }
                        );
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };
        self.loadVDisk = function(vdisk) {
            $.when.apply($, [
                    vdisk.load(),
                    vdisk.fetchVSAGuid()
                ])
                .done(function() {
                    var vm, pool;
                    if (vdisk.vsa() === undefined || vdisk.vsa().guid() !== vdisk.vsaGuid()) {
                        vm = new VMachine(vdisk.vsaGuid());
                        vm.load();
                        vdisk.vsa(vm);
                    }
                    if (vdisk.vMachine() === undefined || vdisk.vMachine().guid() !== vdisk.vMachineGuid()) {
                        vm = new VMachine(vdisk.vMachineGuid());
                        vm.load();
                        vdisk.vMachine(vm);
                    }
                    if (vdisk.vpool() === undefined || vdisk.vpool().guid() !== vdisk.vpoolGuid()) {
                        pool = new VPool(vdisk.vpoolGuid());
                        pool.load();
                        vdisk.vpool(pool);
                    }
                });
        };

        self.rollback = function(guid) {
            var i, vdisks = self.vDisks();
            for (i = 0; i < vdisks.length; i += 1) {
                if (vdisks[i].guid() === guid) {
                    dialog.show(new RollbackWizard({
                        modal: true,
                        type: 'vdisk',
                        guid: guid
                    }));
                }
            }
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(self.load, 5000);
            self.refresher.run();
            self.refresher.start();
            self.shared.footerData(self.vDisks);
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
