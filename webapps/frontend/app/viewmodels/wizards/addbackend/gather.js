// Copyright 2014 iNuron NV
//
// Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/OVS_NON_COMMERCIAL
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define([
    'jquery', 'knockout',
    'plugins/router',
    'ovs/api', 'ovs/shared', 'ovs/generic',
    './data',
    '../../containers/backend', '../../containers/backendtype'
], function($, ko, router, api, shared, generic, data, Backend, BackendType) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data   = data;
        self.shared = shared;

        //Handles
        self.loadBackendsHandle       = undefined;
        self.loadBackendTypesHandle   = undefined;
        self.loadStorageRoutersHandle = undefined;

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, reasons = [], fields = [];
            if (!self.data.name.valid()) {
                valid = false;
                fields.push('name');
                reasons.push($.t('ovs:wizards.addbackend.gather.invalidname'));
            }
            if (self.data.validStorageRouterFound() === false) {
                valid = false;
                reasons.push($.t('ovs:wizards.addbackend.gather.missing_arakoon'));
            }
            if (self.data.storageRoutersChecked() !== true) {
                valid = false;
            }
            $.each(self.data.backends(), function(index, backend) {
                if (backend.name() === self.data.name() && !fields.contains('name')) {
                    valid = false;
                    fields.push('name');
                    reasons.push($.t('ovs:wizards.addbackend.gather.duplicatename'));
                }
            });
            return { value: valid, reasons: reasons, fields: fields };
        });

        // Functions
        self.finish = function() {
            return $.Deferred(function(deferred) {
                var postData = {
                    data: {
                        name: self.data.name(),
                        backend_type_guid: self.data.backendType().guid()
                    }
                };
                api.post('backends', postData)
                    .done(function(data) {
                        router.navigate(shared.routing.loadHash('backend-' + self.data.backendType().code() + '-detail', { guid: data.guid }));
                    })
                    .fail(function() {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.addbackend.gather.failed')
                        );
                    })
                    .always(deferred.resolve);
            }).promise();
        };

        // Durandal
        self.activate = function() {
            if (generic.xhrCompleted(self.loadBackendsHandle)) {
                var options = {
                    sort: 'name',
                    contents: ''
                };
                self.loadBackendsHandle = api.get('backends', { queryparams: options })
                    .done(function (data) {
                        var guids = [], bdata = {};
                        $.each(data.data, function (index, item) {
                            guids.push(item.guid);
                            bdata[item.guid] = item;
                        });
                        generic.crossFiller(
                            guids, self.data.backends,
                            function (guid) {
                                return new Backend(guid);
                            }, 'guid'
                        );
                        $.each(self.data.backends(), function (index, backend) {
                            if ($.inArray(backend.guid(), guids) !== -1) {
                                backend.fillData(bdata[backend.guid()]);
                            }
                        });
                    });
            }
            if (generic.xhrCompleted(self.loadStorageRoutersHandle)) {
                self.loadStorageRoutersHandle = api.get('storagerouters', { queryparams: { contents: '' } })
                    .done(function(data) {
                        var subcalls = [];
                        $.each(data.data, function(index, item) {
                            subcalls.push($.Deferred(function(deferred) {
                                api.post('storagerouters/' + item.guid + '/get_metadata')
                                    .then(self.shared.tasks.wait)
                                    .done(function(metadata) {
                                        $.each(metadata.partitions, function(role, partitions) {
                                            if (role === 'DB' && partitions.length > 0) {
                                                self.data.validStorageRouterFound(true);
                                            }
                                        });
                                        deferred.resolve();
                                    })
                                    .fail(deferred.resolve);
                                }).promise());
                        });
                        $.when.apply($, subcalls)
                            .done(function(){
                                if (self.data.validStorageRouterFound() === undefined) {
                                    self.data.validStorageRouterFound(false);
                                }
                            })
                            .always(function() {
                                self.data.storageRoutersChecked(true);
                            });
                    });
            }
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadBackendTypesHandle)) {
                    var options = {
                        sort: 'name',
                        contents: '',
                        query: JSON.stringify({
                            type: 'AND',
                            items: [['has_plugin', 'EQUALS', true]]
                        })
                    };
                    self.loadBackendTypesHandle = api.get('backendtypes', { queryparams: options })
                        .done(function(data) {
                            var guids = [], btdata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                btdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.data.backendTypes,
                                function(guid) {
                                    return new BackendType(guid);
                                }, 'guid'
                            );
                            $.each(self.data.backendTypes(), function(index, backendType) {
                                if ($.inArray(backendType.guid(), guids) !== -1) {
                                    backendType.fillData(btdata[backendType.guid()]);
                                }
                            });
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
    };
});
