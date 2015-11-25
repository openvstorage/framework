// Copyright 2015 iNuron NV
//
// Licensed under the Open vStorage Modified Apache License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define([
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/failuredomain'
], function($, app, dialog, ko, shared, generic, Refresher, api, FailureDomain) {
    "use strict";
    return function () {
        var self = this;

        // Variables
        self.widgets       = [];
        self.shared        = shared;
        self.guard         = { authenticated: true, registered: true };
        self.domainHeaders = [
            { key: 'name',    value: $.t('ovs:generic.name'),    width: 250       },
            { key: 'address', value: $.t('ovs:generic.address'), width: 250       },
            { key: 'city',    value: $.t('ovs:generic.city'),    width: 250       },
            { key: 'country', value: $.t('ovs:generic.country'), width: undefined },
            { key: undefined, value: $.t('ovs:generic.actions'), width: 60        }
        ];

        // Observables
        self.domains   = ko.observableArray([]);
        self.newDomain = ko.observable();
        self.trigger   = ko.observable(0);

        // Handles
        self.domainsHandle = {};

        // Functions
        self.buildDomain = function() {
            return new FailureDomain();
        };
        self.loadDomains = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.domainsHandle[page])) {
                    var options = {
                        sort: 'name',
                        contents: '_relations'
                    };
                    self.domainsHandle[page] = api.get('failure_domains', { queryparams: options })
                        .then(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    return new FailureDomain(guid);
                                }
                            });
                        })
                        .fail(function() { deferred.reject(); });
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.createDomain = function() {
            self.newDomain().save()
                .then(function(guid) {
                    var domain = new FailureDomain(guid);
                    domain.load();
                    self.trigger(self.trigger() + 1);
                })
                .fail(function(error) {
                    error = $.parseJSON(error.responseText);
                    generic.alertError(
                        $.t('ovs:generic.error'),
                        $.t('ovs:failuredomains.new.addfailed', { why: error.detail })
                    );
                })
                .always(function() {
                    self.newDomain(self.buildDomain());
                });
        };
        self.deleteDomain = function(guid) {
            $.each(self.domains(), function(i, domain) {
                if (domain.guid() === guid && domain.canDelete()) {
                    app.showMessage(
                        $.t('ovs:failuredomains.delete.delete', { what: domain.name() }),
                        $.t('ovs:generic.areyousure'),
                        [$.t('ovs:generic.no'), $.t('ovs:generic.yes')]
                    )
                    .done(function(answer) {
                        if (answer === $.t('ovs:generic.yes')) {
                            api.del('failure_domains/' + guid)
                                .done(function () {
                                    generic.alertSuccess(
                                        $.t('ovs:failuredomains.delete.complete'),
                                        $.t('ovs:failuredomains.delete.deletesuccess')
                                    );
                                    self.domains.remove(domain);
                                })
                                .fail(function (error) {
                                    error = $.parseJSON(error.responseText);
                                    generic.alertError(
                                        $.t('ovs:generic.error'),
                                        $.t('ovs:failuredomains.clients.deletefailed', { why: error.detail })
                                    );
                                });
                        }
                    });
                }
            });
        };

        // More observables
        self.newDomain(self.buildDomain());

        // Durandal
        self.deactivate = function() {
            $.each(self.widgets, function(i, item) {
                item.deactivate();
            });
        };
    };
});
