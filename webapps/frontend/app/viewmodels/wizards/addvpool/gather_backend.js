// Copyright 2016 iNuron NV
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
    'jquery', 'knockout',
    'ovs/api', 'ovs/generic',
    '../../containers/albabackend', './data'
], function($, ko, api, generic, AlbaBackend, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data = data;

        // Handles
        self.fetchAlbaVPoolHandle = undefined;

        // Observables
        self.albaBackendLoading = ko.observable(false);
        self.albaPresetMap      = ko.observable({});
        self.invalidAlbaInfo    = ko.observable(false);

        // Computed
        self.localBackendsAvailable = ko.computed(function() {
            if (self.data.localHost() && self.data.albaBackends().length < 2) {
                if (self.data.editBackend()) {
                    self.data.aaLocalHost(false);
                }
                return false;
            }
            if (self.data.editBackend()) {
                self.data.aaLocalHost(true);
            }
            return true;
        });
        self.isPresetAvailable = ko.computed(function() {
            var presetAvailable = true;
            if (self.data.albaAABackend() !== undefined && self.data.albaAAPreset() !== undefined) {
                var guid = self.data.albaAABackend().guid(),
                    name = self.data.albaAAPreset().name;
                if (self.albaPresetMap().hasOwnProperty(guid) && self.albaPresetMap()[guid].hasOwnProperty(name)) {
                    presetAvailable = self.albaPresetMap()[guid][name];
                }
            }
            return presetAvailable;
        });
        self.canContinue = ko.computed(function() {
            var valid = true, showErrors = false, reasons = [], fields = [];
            if (!self.data.useAA()) {
                return { value: valid, showErrors: showErrors, reasons: reasons, fields: fields };
            }
            if (self.data.backend() === 'alba' && self.data.editBackend()) {
                if (self.data.albaAABackend() === undefined) {
                    valid = false;
                    reasons.push($.t('ovs:wizards.add_vpool.gather_backend.choose_backend'));
                    fields.push('backend');
                }
                if (!self.data.aaLocalHost()) {
                    if (!self.data.aaHost.valid()) {
                        valid = false;
                        fields.push('host');
                        reasons.push($.t('ovs:wizards.add_vpool.gather_backend.invalid_host'));
                    }
                    if (self.data.aaAccesskey() === '' || self.data.aaSecretkey() === '') {
                        valid = false;
                        fields.push('clientid');
                        fields.push('clientsecret');
                        reasons.push($.t('ovs:wizards.add_vpool.gather_backend.no_credentials'));
                    }
                    if (self.invalidAlbaInfo()) {
                        valid = false;
                        reasons.push($.t('ovs:wizards.add_vpool.gather_backend.invalid_alba_info'));
                        fields.push('clientid');
                        fields.push('clientsecret');
                        fields.push('host');
                    }
                }
            }
            return { value: valid, showErrors: showErrors, reasons: reasons, fields: fields };
        });

        // Functions
        self.shouldSkip = function() {
            return $.Deferred(function(deferred) {
                if (self.data.backend() === 'alba' && self.data.editBackend()) {
                    deferred.resolve(false);  // Don't skip this page for ALBA backends
                } else {
                    deferred.resolve(true);
                }
            }).promise();
        };
        self.loadAlbaBackends = function() {
            return $.Deferred(function(albaDeferred) {
                generic.xhrAbort(self.fetchAlbaVPoolHandle);
                var relay = '', remoteInfo = {},
                    getData = {
                        backend_type: 'alba',
                        contents: '_dynamics'
                    };
                if (!self.data.aaLocalHost()) {
                    relay = 'relay/';
                    remoteInfo.ip = self.data.aaHost();
                    remoteInfo.port = self.data.aaPort();
                    remoteInfo.client_id = self.data.aaAccesskey().replace(/\s+/, "");
                    remoteInfo.client_secret = self.data.aaSecretkey().replace(/\s+/, "");
                }
                $.extend(getData, remoteInfo);
                self.albaBackendLoading(true);
                self.invalidAlbaInfo(false);
                self.fetchAlbaVPoolHandle = api.get(relay + 'backends', { queryparams: getData })
                    .done(function(data) {
                        var available_backends = [], calls = [];
                        $.each(data.data, function (index, item) {
                            if (item.available === true) {
                                calls.push(
                                    api.get(relay + 'alba/backends/' + item.linked_guid + '/', { queryparams: getData })
                                        .then(function(data) {
                                            if (data.available === true && data.guid !== self.data.albaBackend().guid()) {
                                                var asdsFound = false;
                                                $.each(data.asd_statistics, function(key, value) {  // As soon as we enter loop, we know at least 1 ASD is linked to this backend
                                                    asdsFound = true;
                                                    return false;
                                                });
                                                if (asdsFound === true) {
                                                    available_backends.push(data);
                                                    self.albaPresetMap()[data.guid] = {};
                                                    $.each(data.presets, function (_, preset) {
                                                        self.albaPresetMap()[data.guid][preset.name] = preset.is_available;
                                                    });
                                                }
                                            }
                                        })
                                );
                            }
                        });
                        $.when.apply($, calls)
                            .then(function() {
                                if (available_backends.length > 0) {
                                    var guids = [], abData = {};
                                    $.each(available_backends, function(index, item) {
                                        guids.push(item.guid);
                                        abData[item.guid] = item;
                                    });
                                    generic.crossFiller(
                                        guids, self.data.albaAABackends,
                                        function(guid) {
                                            return new AlbaBackend(guid);
                                        }, 'guid'
                                    );
                                    $.each(self.data.albaAABackends(), function(index, albaBackend) {
                                        albaBackend.fillData(abData[albaBackend.guid()]);
                                    });
                                    self.data.albaAABackend(self.data.albaAABackends()[0]);
                                    self.data.albaAAPreset(self.data.albaAABackends()[0].enhancedPresets()[0]);
                                } else {
                                    self.data.albaAABackends([]);
                                    self.data.albaAABackend(undefined);
                                    self.data.albaAAPreset(undefined);
                                }
                                self.albaBackendLoading(false);
                            })
                            .done(albaDeferred.resolve)
                            .fail(function() {
                                self.data.albaAABackends([]);
                                self.data.albaAABackend(undefined);
                                self.data.albaAAPreset(undefined);
                                self.albaBackendLoading(false);
                                self.invalidAlbaInfo(true);
                                albaDeferred.reject();
                            });
                    })
                    .fail(function() {
                        self.data.albaAABackends([]);
                        self.data.albaAABackend(undefined);
                        self.data.albaAAPreset(undefined);
                        self.albaBackendLoading(false);
                        self.invalidAlbaInfo(true);
                        albaDeferred.reject();
                    });
            }).promise();
        };

        // Durandal
        self.activate = function() {
            if (self.data.albaBackend() !== undefined && self.data.albaAABackend() !== undefined && self.data.albaBackend().guid() === self.data.albaAABackend().guid()) {
                self.data.albaAABackends([]);
                $.each(self.data.albaBackends(), function (_, backend) {
                    if (backend !== self.data.albaBackend() && !self.data.albaAABackends().contains(backend)) {
                        self.data.albaAABackends().push(backend);
                    }
                });
                if (self.data.albaAABackends().length === 0) {
                    self.data.albaAABackend(undefined);
                    self.data.albaAAPreset(undefined);
                } else {
                    self.data.albaAABackend(self.data.albaAABackends()[0]);
                    self.data.albaAAPreset(self.data.albaAABackends()[0].enhancedPresets()[0]);
                }
            }
        };
    };
});

