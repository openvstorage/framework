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
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    'viewmodels/containers/domain/domain'
], function($, app, dialog, ko, shared, generic, Refresher, api, Domain) {
    "use strict";
    return function () {
        var self = this;

        // Variables
        self.widgets       = [];
        self.shared        = shared;
        self.guard         = { authenticated: true };
        self.domainHeaders = [
            { key: 'name',    value: $.t('ovs:generic.name'),    width: undefined },
            { key: undefined, value: $.t('ovs:generic.actions'), width: 60        }
        ];

        // Observables
        self.domains   = ko.observableArray([]);
        self.newDomain = ko.observable();
        self.trigger   = ko.observable(0);

        // Handles
        self.domainsHandle = {};

        // Computed
        self.domainNames = ko.computed(function() {
            var names = [];
            $.each(self.domains(), function(i, domain) {
                names.push(domain.name().toLowerCase());
            });
            return names;
        });

        // Functions
        self.buildDomain = function() {
            var domain = new Domain();
            domain.existingDomainNames = self.domainNames;
            return domain;
        };
        self.loadDomains = function(options) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.domainsHandle[options.page])) {
                    options.sort = 'name';
                    options.contents = '_relations';
                    self.domainsHandle[options.page] = api.get('domains', { queryparams: options })
                        .then(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    var domain = new Domain(guid);
                                    domain.existingDomainNames = self.domainNames;
                                    return domain;
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
                    var domain = new Domain(guid);
                    domain.load();
                    self.trigger(self.trigger() + 1);
                })
                .fail(function(error) {
                    error = generic.extractErrorMessage(error);
                    generic.alertError(
                        $.t('ovs:generic.error'),
                        $.t('ovs:domains.new.addfailed', { why: error })
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
                        $.t('ovs:domains.delete.delete', { what: domain.name() }),
                        $.t('ovs:generic.are_you_sure'),
                        [$.t('ovs:generic.no'), $.t('ovs:generic.yes')]
                    )
                    .done(function(answer) {
                        if (answer === $.t('ovs:generic.yes')) {
                            api.del('domains/' + guid)
                                .done(function () {
                                    generic.alertSuccess(
                                        $.t('ovs:domains.delete.complete'),
                                        $.t('ovs:domains.delete.deletesuccess')
                                    );
                                    self.domains.remove(domain);
                                })
                                .fail(function (error) {
                                    error = generic.extractErrorMessage(error);
                                    generic.alertError(
                                        $.t('ovs:generic.error'),
                                        $.t('ovs:domains.delete.deletefailed', { why: error })
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
