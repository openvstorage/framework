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
    'viewmodels/wizards/addvpool/gather_base'
], function ($, ko,
             BaseStep) {
    "use strict";

    /**
     * Base container for the vpool steps
     * All reusable storagerouter logic is offloaded to this model
     * @param options: Step options given
     * @constructor
     */
    function BaseStepConfig(options) {
        var self = this;

        // Variables
        BaseStep.call(self, options);
        self.actived = false;
        self._reUsedStorageRouter = ko.observable();

        // Computed
        self.canContinue = ko.pureComputed(function() {
            var reasons = [], fields = [];
            var cacheType = self.getCacheType();
            var fragmentCache = self.data.cachingData[cacheType];
            var baseTranslation = 'ovs:wizards.add_vpool.gather_{0}'.format([cacheType]);
            if (fragmentCache.isUsed() === true){
                if (self.data.loadingBackends() === true) {
                    reasons.push($.t(baseTranslation + '.backends_loading'));
                } else {
                    var connectionInfo = self.getConnectionInfo();
                    if (fragmentCache.is_backend()){
                        if (!self.backend()) {
                            reasons.push($.t(baseTranslation + '.choose_backend'));
                            fields.push('backend');
                        } else if (!self.preset()) {
                            reasons.push($.t(baseTranslation + '.choose_preset'));
                            fields.push('preset');
                        }
                        if (!connectionInfo.isLocalBackend() && !connectionInfo.hasRemoteInfo() || self.data.invalidBackendInfo()) {
                            reasons.push($.t(baseTranslation + '.invalid_alba_info'));
                            fields.push('invalid_alba_info');
                        }
                    }
                }
            }
            return { value: reasons.length === 0, reasons: reasons, fields: fields };
        });

        self.reUsedStorageRouter = ko.computed({
            deferEvaluation: true,  // Wait with computing for an actual subscription
            read: function() {
                return self._reUsedStorageRouter()
            },
            write: function(data) {
                self._reUsedStorageRouter(data);
                // Set connection info
                if (data) {
                    var cacheConnectionInfoMapping = self.data.vPool().getCacheConnectionInfoMapping();
                    var storagerouterConnectionInfo = cacheConnectionInfoMapping[self.getCacheType()][data.guid()];
                    self.getConnectionInfo().update(storagerouterConnectionInfo)
                }
            }
        });
        self.reUsableStorageRouters = ko.pureComputed(function() {
            var mapping = self.data.vPool().getCacheConnectionInfoMapping();
            var storagerouters = [];
            $.each(self.data.storageRoutersUsed(), function(index, storagerouter) {
                if (storagerouter.guid() in mapping[self.getCacheType()]) {
                    storagerouters.push(storagerouter)
                }
            });
            storagerouters.unshift(undefined);  // Insert undefined as element 0
            return storagerouters;
        });


        // Functions
        /**
         * Copy the connection info from a previous step
         */
        self.copyConnectionInfo = function(){
            var currectConnectionInfo = self.getConnectionInfo();
            var otherConnectionInfo = self.getOtherConnectionInfo();
            currectConnectionInfo.update(otherConnectionInfo.toJS())
        };

        // Abstract. Requires implementations
        self.getCacheType = function() {
            throw new Error("Method must be implemented.");
        };
        self.getOtherConnectionInfo = function() {
            throw new Error("Method must be implemented.");
        };

        // Abstract implementations
        self.getBackendInfo = function() {
            return self.data.cachingData[self.getCacheType()].backend_info;
        };
        // Durandal
        self.activate = function() {
            if (self.actived === false) {
                var connectionInfo = self.getConnectionInfo();
                if (!ko.utils.unwrapObservable(connectionInfo.isLocalBackend) && !connectionInfo.host()) {
                    self.data.loadBackends(connectionInfo);
                }
                self.actived = true;
            }
        };
    }
    return BaseStepConfig;
});
