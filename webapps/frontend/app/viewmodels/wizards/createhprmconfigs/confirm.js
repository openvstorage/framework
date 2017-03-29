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
        self.data   = data;
        self.shared = shared;

        // Handles
        self.generateConfigFiles = undefined;

        // Computed
        self.canContinue = ko.computed(function() {
            return { value: true, reasons: [], fields: [] };
        });

        // Functions
        self.finish = function() {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.generateConfigFiles);
                var parameters = {
                    port: self.data.hprmPort(),
                    vpool_guid: self.data.vPool().guid(),
                    fragment_cache_on_read: self.data.cacheOnRead(),
                    fragment_cache_on_write: self.data.cacheOnWrite()
                };
                if (self.data.cacheOnRead() || self.data.cacheOnWrite()) {
                    if (self.data.cacheUseAlba() === false) {
                        parameters.path = self.data.localPath();
                        parameters.size = self.data.localSize();
                    } else {
                        parameters.connection_info = {
                            host: self.data.albaHost(),
                            port: self.data.albaPort(),
                            local: self.data.albaUseLocalBackend(),
                            client_id: self.data.albaClientID(),
                            client_secret: self.data.albaClientSecret()
                        };
                        parameters.backend_info = {
                            preset: self.data.albaPreset().name,
                            alba_backend_guid: self.data.albaBackend().guid,
                            alba_backend_name: self.data.albaBackend().name
                        };
                    }
                }
                generic.alertInfo($.t('ovs:wizards.create_hprm_configs.confirm.started'),
                                  $.t('ovs:wizards.create_hprm_configs.confirm.started_msg', {vpool: self.data.vPool().name(),
                                                                                              storagerouter: self.data.storageRouter().name()}));
                api.get('storagerouters/' + self.data.storageRouter().guid() + '/create_hprm_config_files', {queryparams: {parameters: JSON.stringify(parameters)}})
                    .then(self.shared.tasks.wait)
                    .done(function(data) {
                        window.location.href = 'downloads/' + data;
                        if (self.data.completed !== undefined) {
                            self.data.completed.resolve(true);
                        }
                        generic.alertSuccess($.t('ovs:wizards.create_hprm_configs.confirm.success'),
                                             $.t('ovs:wizards.create_hprm_configs.confirm.success_msg', {vpool: self.data.vPool().name(),
                                                                                                         storagerouter: self.data.storageRouter().name()}));
                    })
                    .fail(function(error) {
                        if (self.data.completed !== undefined) {
                            self.data.completed.resolve(false);
                        }
                        error = generic.extractErrorMessage(error);
                        generic.alertError($.t('ovs:generic.error'),
                                           $.t('ovs:wizards.create_hprm_configs.confirm.failure_msg', {why: error,
                                                                                                       vpool: self.data.vPool().name(),
                                                                                                       storagerouter: self.data.storageRouter().name()})
                        );
                    });
                deferred.resolve();
            }).promise();
        };
    };
});
