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
        self.instructions = ko.computed(function() {
            return $.t('ovs:wizards.create_hprm_configs.confirm.instruction_steps', {identifier: self.data.identifier()});
        });

        // Functions
        self.formatFloat = function(value) {
            return parseFloat(value);
        };
        self.finish = function() {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.generateConfigFiles);
                var parameters = {
                    port: self.data.hprmPort(),
                    identifier: self.data.identifier(),
                    fragment: {
                        read: self.data.fragmentCacheOnRead(),
                        write: self.data.fragmentCacheOnWrite()
                    },
                    block: {
                        read: self.data.blockCacheOnRead(),
                        write: self.data.blockCacheOnWrite()
                    }
                };
                if (self.data.fragmentCacheOnRead() || self.data.fragmentCacheOnWrite()) {
                    if (self.data.useFC() === false) {
                        parameters.fragment.path = self.data.localPathFC();
                        parameters.fragment.size = self.data.localSizeFC();
                    } else {
                        parameters.fragment.connection_info = {
                            host: self.data.hostFC(),
                            port: self.data.portFC(),
                            local: self.data.localHostFC(),
                            client_id: self.data.clientIDFC(),
                            client_secret: self.data.clientSecretFC()
                        };
                        parameters.fragment.backend_info = {
                            preset: self.data.presetFC().name,
                            alba_backend_guid: self.data.backendFC().guid,
                            alba_backend_name: self.data.backendFC().name
                        };
                    }
                }
                if (self.data.supportsBC() && (self.data.blockCacheOnRead() || self.data.blockCacheOnWrite())) {
                    if (self.data.useBC() === false) {
                        parameters.block.path = self.data.localPathBC();
                        parameters.block.size = self.data.localSizeBC();
                    } else {
                        parameters.block.connection_info = {
                            host: self.data.hostBC(),
                            port: self.data.portBC(),
                            local: self.data.localHostBC(),
                            client_id: self.data.clientIDBC(),
                            client_secret: self.data.clientSecretBC()
                        };
                        parameters.block.backend_info = {
                            preset: self.data.presetBC().name,
                            alba_backend_guid: self.data.backendBC().guid,
                            alba_backend_name: self.data.backendBC().name
                        };
                    }
                }
                generic.alertInfo($.t('ovs:wizards.create_hprm_configs.confirm.started'),
                                  $.t('ovs:wizards.create_hprm_configs.confirm.started_msg', {vpool: self.data.vPool().name()}));
                api.post('vpools/' + self.data.vPool().guid() + '/create_hprm_config_files', {queryparams: {parameters: JSON.stringify(parameters)}})
                    .then(self.shared.tasks.wait)
                    .done(function(data) {
                        window.location.href = 'downloads/' + data;
                        generic.alertSuccess($.t('ovs:wizards.create_hprm_configs.confirm.success'),
                                             $.t('ovs:wizards.create_hprm_configs.confirm.success_msg', {vpool: self.data.vPool().name()}));
                    })
                    .fail(function(error) {
                        error = generic.extractErrorMessage(error);
                        generic.alertError($.t('ovs:generic.error'),
                                           $.t('ovs:wizards.create_hprm_configs.confirm.failure_msg', {why: error,
                                                                                                       vpool: self.data.vPool().name()})
                        );
                    });
                deferred.resolve();
            }).promise();
        };
    };
});
