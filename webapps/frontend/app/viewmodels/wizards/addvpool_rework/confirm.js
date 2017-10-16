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
        self.postData = ko.observable();
        // Computed
        self.canContinue = ko.pureComputed(function() {
            return { value: true, reasons: [], fields: [] };
        });

        // Functions
        self.getPostData = function() {
            var vpoolBackendInfo = self.data.backendData.backend_info;
            var fragmentCacheData = self.data.cachingData.fragment_cache;
            var blockCacheData = self.data.cachingData.block_cache;
            var cacheQuotaFC = fragmentCacheData.quota();
            var cacheQuotaBC = blockCacheData.quota();
            var postData = {
                call_parameters: {
                    // VPool - StorageDriver related
                    vpool_name: self.data.vPool().name(),
                    storagerouter_ip: self.data.storageRouter().ipAddress(),
                    // VPool backend info
                    backend_info: {
                        preset: vpoolBackendInfo.preset(),
                        alba_backend_guid: vpoolBackendInfo.alba_backend_guid()
                    },
                    connection_info: vpoolBackendInfo.connection_info.toJS(),
                    config_params: {
                        dtl_mode: (self.data.configParams.dtl_enabled() === true ? self.data.configParams.dtl_mode(): 'no_sync'),
                        sco_size: self.data.configParams.sco_size(),
                        cluster_size: self.data.configParams.cluster_size(),
                        write_buffer: self.data.configParams.write_buffer(),
                        dtl_transport: self.data.configParams.dtl_transport()
                    },
                    storage_ip: self.data.storageDriverParams.storageIP(),
                    writecache_size: self.data.storageDriverParams.globalWriteBuffer(),
                    mds_config_params: {
                        mds_safety: self.data.configParams.mds_config.mds_safety()
                    },
                    parallelism: {
                        proxies: self.data.storageDriverParams.proxyAmount()
                    },
                    // Cache related
                    // Fragment Cache - Backend data will be added later
                    fragment_cache_on_read: fragmentCacheData.read(),
                    fragment_cache_on_write: fragmentCacheData.write(),
                    cache_quota_fc: cacheQuotaFC !== undefined ? Math.round(cacheQuotaFC * Math.pow(1024, 3)) : undefined,
                    // Block Cache - Backend data will be added later
                    block_cache_on_read: blockCacheData.read(),
                    block_cache_on_write: blockCacheData.write(),
                    cache_quota_bc: cacheQuotaBC !== undefined ? Math.round(cacheQuotaBC * Math.pow(1024, 3)) : undefined
                }
            };
            // Add caching backend data where needed
            $.each([{suffix: 'fc', data: fragmentCacheData}, {suffix: 'bc', data: blockCacheData}], function(index, cacheData) {
                var cachingData = cacheData.data;
                if (cachingData.is_backend()) {
                    var connectionInfo = cachingData.backend_info.connection_info.toJS();
                    var backendInfo = {
                        preset: cachingData.backend_info.preset(),
                        alba_backend_guid: cachingData.backend_info.alba_backend_guid()
                    };
                    postData.call_parameters['connection_info_{0}'.format([cacheData.suffix])] = connectionInfo;
                    postData.call_parameters['backend_info_{0}'.format([cacheData.suffix])] = backendInfo;

                }
            });
            return postData
        };
        self.formatFloat = function(value) {
            return parseFloat(value);
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
            var postData = self.getPostData();
            self.activated = true;
        }
    };
});
