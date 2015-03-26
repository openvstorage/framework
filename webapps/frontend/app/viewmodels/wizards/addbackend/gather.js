// Copyright 2014 CloudFounders NV
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
    'plugins/router',
    'ovs/api', 'ovs/shared', 'ovs/generic',
    './data',
    '../../containers/backend', '../../containers/backendtype'
], function($, ko, router, api, shared, generic, data, Backend, BackendType) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data                   = data;
        self.shared                 = shared;
        self.loadBackendTypesHandle = undefined;
        self.loadBackendsHandle     = undefined;

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, reasons = [], fields = [];
            if (!self.data.name.valid()) {
                valid = false;
                fields.push('name');
                reasons.push($.t('ovs:wizards.addbackend.gather.invalidname'));
            }
            $.each(self.data.backends(), function(index, backend) {
                if (backend.name() === self.data.name() && $.inArray('name', fields) === -1) {
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
