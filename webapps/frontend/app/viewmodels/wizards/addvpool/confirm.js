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
    'ovs/api', 'ovs/generic', 'ovs/shared'
], function($, ko, api, generic, shared) {
    "use strict";
    return function(options) {
        var self = this;

        // Variables
        self.activated = false;
        self.data   = options.data;

        // Observables
        self.loadingUpdateImpact = ko.observable(false);
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
                    caching_info: {
                        // Fragment Cache - Backend data will be added later
                        fragment_cache_on_read: fragmentCacheData.read(),
                        fragment_cache_on_write: fragmentCacheData.write(),
                        cache_quota_fc: cacheQuotaFC !== undefined ? Math.round(cacheQuotaFC * Math.pow(1024, 3)) : undefined,
                        // Block Cache - Backend data will be added later
                        block_cache_on_read: blockCacheData.read(),
                        block_cache_on_write: blockCacheData.write(),
                        cache_quota_bc: cacheQuotaBC !== undefined ? Math.round(cacheQuotaBC * Math.pow(1024, 3)) : undefined
                    }
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
                    postData.call_parameters['backend_info_{0}'.format(cacheData.suffix)] = backendInfo;

                }
            });
            return postData
        };
        self.formatFloat = function(value) {
            return parseFloat(value);
        };
        self.finish = function() {
            var postData = self.getPostData();
            var vpool = self.data.vPool();
            if (self.data.isExtend() === false) {
                generic.alertInfo($.t('ovs:wizards.add_vpool.confirm.started_title'),
                    $.t('ovs:wizards.add_vpool.confirm.started_message', {what: vpool.name()}));
            } else {
                generic.alertInfo($.t('ovs:wizards.extend_vpool.confirm.started_title'),
                    $.t('ovs:wizards.extend_vpool.confirm.started_message', {what: vpool.name()}));
            }
            return api.post('storagerouters/' + self.data.storageRouter().guid() + '/add_vpool', {data: postData})
                .then(shared.tasks.wait)
                // Using then instead of deferred to chain the returned promise
                .then(function (data) {
                    // Success
                    if (self.data.isExtend() === false) {
                        generic.alertSuccess($.t('ovs:wizards.add_vpool.confirm.success_title'),
                            $.t('ovs:wizards.add_vpool.confirm.success_message', {what: vpool.name()}));
                    } else {
                        generic.alertSuccess($.t('ovs:wizards.extend_vpool.confirm.success_title'),
                            $.t('ovs:wizards.extend_vpool.confirm.success_message', {what: vpool.name()}));
                    }
                    return data;
                }, function (error) {
                    // fail
                    error = generic.extractErrorMessage(error);
                    if (self.data.isExtend() === false) {
                        generic.alertError($.t('ovs:wizards.add_vpool.confirm.failure_title'),
                            $.t('ovs:wizards.add_vpool.confirm.failure_message', {what: vpool.name(), why: error}));
                    } else {
                        generic.alertError($.t('ovs:wizards.extend_vpool.confirm.failure_title'),
                            $.t('ovs:wizards.extend_vpool.confirm.failure_message', {
                                what: vpool.name(),
                                why: error
                            }));
                    }
                    return error;
                });
        };

        // Durandal
        self.activate = function() {
            if (self.activated === true){
                return
            }
            self.activated = true;
        }
    };
});
