// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vpool'
], function($, app, dialog, ko, shared, generic, Refresher, api, VPool) {
    "use strict";
    return function() {
        var self = this;

        // System
        self.shared    = shared;
        self.guard     = { authenticated: true };
        self.refresher = new Refresher();
        self.widgets   = [];

        // Data
        self.vPoolHeaders = [
            { key: 'name',              value: $.t('ovs:generic.name'),             width: 150,       colspan: undefined },
            { key: 'storedData',        value: $.t('ovs:generic.storeddata'),       width: 100,       colspan: undefined },
            { key: 'freeSpace',         value: $.t('ovs:vpools.freespace'),         width: 100,       colspan: undefined },
            { key: 'cacheRatio',        value: $.t('ovs:generic.cache'),            width: 100,       colspan: undefined },
            { key: 'iops',              value: $.t('ovs:generic.iops'),             width: 55,        colspan: undefined },
            { key: 'backendType',       value: $.t('ovs:vpools.backendtype'),       width: 100,       colspan: undefined },
            { key: 'backendConnection', value: $.t('ovs:vpools.backendconnection'), width: 100,       colspan: undefined },
            { key: 'backendLogin',      value: $.t('ovs:vpools.backendlogin'),      width: undefined, colspan: undefined }
        ];
        self.vPools = ko.observableArray([]);
        self.vPoolGuids = [];

        // Variables
        self.loadVPoolsHandle = undefined;

        // Functions
        self.load = function() {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.loadVPoolsHandle);
                self.loadVPoolsHandle = api.get('vpools')
                    .done(function(data) {
                        var i, guids = [];
                        for (i = 0; i < data.length; i += 1) {
                            guids.push(data[i].guid);
                        }
                        generic.crossFiller(
                            guids, self.vPoolGuids, self.vPools,
                            function(guid) {
                                return new VPool(guid);
                            }, 'guid'
                        );
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };
        self.loadVPool = function(vpool) {
            return $.Deferred(function(deferred) {
                vpool.load()
                    .done(function() {
                        // (Re)sort vPools
                        generic.advancedSort(self.vPools, ['name', 'guid']);
                    })
                    .always(deferred.resolve);
            }).promise();
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(self.load, 5000);
            self.refresher.run();
            self.refresher.start();
            self.shared.footerData(self.vPools);
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
