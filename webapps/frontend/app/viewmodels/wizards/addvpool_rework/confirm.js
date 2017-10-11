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
    './data',
    'ovs/api', 'ovs/generic', 'ovs/shared'
], function($, ko, data, api, generic, shared) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.activated = false;
        self.data   = data;
        self.shared = shared;

        // Observables
        self.loadingUpdateImpact = ko.observable(false);
        // Computed
        self.canContinue = ko.pureComputed(function() {
            return { value: true, reasons: [], fields: [] };
        });

        // Functions
        self.formatFloat = function(value) {
            return parseFloat(value);
        };
        self.calculateUpdateImpact = function () {
            var postData = {
                vpool_updates: ko.mapping.toJS(self.data.configParams),
                storagedriver_updates: ko.mapping.toJS(self.data.cachingData)
            };
            postData.storagedriver_updates.proxy_amount = self.data.proxyAmount();
            postData.storagedriver_updates.global_write_buffer = self.data.globalWriteBuffer();
            self.loadingUpdateImpact(true);
            return api.post('storagedrivers/{0}/calculate_update_impact'.format([self.data.storageDriver().guid()]), {data: postData})
                .done(function(data) {
                    console.log(data)
                })
                .always(function() {
                    self.loadingUpdateImpact(false);
                })
        };
        self.finish = function() {
            return $.Deferred(function (deferred) {
                deferred.resolve()
            }).promise()
        };
            // return $.Deferred(function(deferred) {
            //     var postData = {
            //         call_parameters: {
            //             vpool_name: self.data.name(),
            //             storage_ip: self.data.storageIP(),
            //             cache_quota_fc: self.data.useFC() === true && self.data.cacheQuotaFC() !== undefined && self.data.cacheQuotaFC() !== '' ? Math.round(self.data.cacheQuotaFC() * Math.pow(1024, 3)) : undefined,
            //             cache_quota_bc: self.data.useBC() === true && self.data.cacheQuotaBC() !== undefined && self.data.cacheQuotaBC() !== '' ? Math.round(self.data.cacheQuotaBC() * Math.pow(1024, 3)) : undefined,
            //             writecache_size: self.data.writeBufferGlobal(),
            //             storagerouter_ip: self.data.storageRouter().ipAddress(),
            //             fragment_cache_on_read: self.data.fragmentCacheOnRead(),
            //             fragment_cache_on_write: self.data.fragmentCacheOnWrite(),
            //             connection_info: {
            //                 host: self.data.host(),
            //                 port: self.data.port(),
            //                 local: self.data.localHost(),
            //                 client_id: self.data.clientID(),
            //                 client_secret: self.data.clientSecret()
            //             },
            //             backend_info: {
            //                 preset: (self.data.preset() !== undefined ? self.data.preset().name : undefined),
            //                 alba_backend_guid: (self.data.backend() !== undefined ? self.data.backend().guid : undefined)
            //             },
            //             block_cache_on_read: self.data.blockCacheOnRead() === undefined ? false : self.data.blockCacheOnRead(),
            //             block_cache_on_write: self.data.blockCacheOnWrite() === undefined ? false : self.data.blockCacheOnWrite(),
            //             config_params: {
            //                 dtl_mode: (self.data.dtlEnabled() === true ? self.data.dtlMode().name : 'no_sync'),
            //                 sco_size: self.data.scoSize(),
            //                 cluster_size: self.data.clusterSize(),
            //                 write_buffer: self.data.writeBufferVolume(),
            //                 dtl_transport: self.data.dtlTransportMode().name
            //             },
            //             mds_config_params: {
            //                 mds_safety: self.data.mdsSafety()
            //             },
            //             parallelism: {
            //                 proxies: self.data.proxyAmount()
            //             }
            //         }
            //     };
            //
            //     if (self.data.useFC() === true) {
            //         postData.call_parameters.connection_info_fc = {
            //             host: self.data.hostFC(),
            //             port: self.data.portFC(),
            //             local: self.data.localHostFC(),
            //             client_id: self.data.clientIDFC(),
            //             client_secret: self.data.clientSecretFC()
            //         };
            //         postData.call_parameters.backend_info_fc = {
            //             preset: (self.data.presetFC() !== undefined ? self.data.presetFC().name : undefined),
            //             alba_backend_guid: (self.data.backendFC() !== undefined ? self.data.backendFC().guid : undefined)
            //         };
            //     }
            //     if (self.data.useBC() === true && self.data.supportsBC() === true) {
            //         postData.call_parameters.connection_info_bc = {
            //             host: self.data.hostBC(),
            //             port: self.data.portBC(),
            //             local: self.data.localHostBC(),
            //             client_id: self.data.clientIDBC(),
            //             client_secret: self.data.clientSecretBC()
            //         };
            //         postData.call_parameters.backend_info_bc = {
            //             preset: (self.data.presetBC() !== undefined ? self.data.presetBC().name : undefined),
            //             alba_backend_guid: (self.data.backendBC() !== undefined ? self.data.backendBC().guid : undefined)
            //         };
            //     }
            //     (function(name, vpool, completed, dfd) {
            //         if (vpool === undefined) {
            //             generic.alertInfo($.t('ovs:wizards.add_vpool.confirm.started_title'),
            //                               $.t('ovs:wizards.add_vpool.confirm.started_message', { what: name }));
            //         } else {
            //             generic.alertInfo($.t('ovs:wizards.extend_vpool.confirm.started_title'),
            //                               $.t('ovs:wizards.extend_vpool.confirm.started_message', { what: name }));
            //         }
            //         api.post('storagerouters/' + self.data.storageRouter().guid() + '/add_vpool', { data: postData })
            //             .then(self.shared.tasks.wait)
            //             .done(function() {
            //                 if (vpool === undefined) {
            //                     generic.alertSuccess($.t('ovs:wizards.add_vpool.confirm.success_title'),
            //                                          $.t('ovs:wizards.add_vpool.confirm.success_message', { what: name }));
            //                 } else {
            //                     generic.alertSuccess($.t('ovs:wizards.extend_vpool.confirm.success_title'),
            //                                          $.t('ovs:wizards.extend_vpool.confirm.success_message', { what: name }));
            //                 }
            //                 if (completed !== undefined) {
            //                     completed.resolve(true);
            //                 }
            //             })
            //             .fail(function(error) {
            //                 error = generic.extractErrorMessage(error);
            //                 if (vpool === undefined) {
            //                     generic.alertError($.t('ovs:wizards.add_vpool.confirm.failure_title'),
            //                                        $.t('ovs:wizards.add_vpool.confirm.failure_message', { what: name, why: error }));
            //                 } else {
            //                     generic.alertError($.t('ovs:wizards.extend_vpool.confirm.failure_title'),
            //                                        $.t('ovs:wizards.extend_vpool.confirm.failure_message', { what: name, why: error }));
            //                 }
            //                 if (completed !== undefined) {
            //                     completed.resolve(false);
            //                 }
            //             });
            //         dfd.resolve();
            //     })(self.data.name(), self.data.vPool(), self.data.completed, deferred);
            // }).promise();

        // Durandal
        self.activate = function() {
            if (self.activated === true){
                return
            }
            self.calculateUpdateImpact()
            self.activated = true;
        }
    };
});
