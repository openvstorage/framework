// Copyright 2015 Open vStorage NV
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
    'ovs/api', 'ovs/shared', 'ovs/generic',
    './data', '../../containers/failuredomain', '../../containers/storagerouter'
], function($, ko, api, shared, generic, data, FailureDomain, StorageRouter) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data   = data;
        self.shared = shared;

        // Observables
        self.loaded = ko.observable(false);

        // Handles
        self.failureDomainHandle = undefined;
        self.storagerouterHandle = undefined;

        // Computed
        self.data.availablePrimary = ko.computed(function() {
            var primary = [];
            if (self.data.newFailureDomain()) {
                $.each(self.data.allStorageRouters(), function(index, storageRouter) {
                    primary.push({guid: storageRouter.guid(),
                                  name: storageRouter.name(),
                                  disabled: storageRouter.primaryFailureDomain() === undefined ? false : true});
                });
            } else {
                $.each(self.data.allStorageRouters(), function(index, storageRouter) {
                    primary.push({guid: storageRouter.guid(),
                                  name: storageRouter.name(),
                                  disabled: false});
                    if (storageRouter.primaryFailureDomain() !== undefined && storageRouter.primaryFailureDomain().guid() !== self.data.failureDomain().guid()) {
                        primary[index].disabled = true;
                    }
                    $.each(self.data.secondaryStorageRouters(), function(j_index, srInfo) {
                        if (srInfo.guid === storageRouter.guid()) {
                            primary[index].disabled = true;
                        }
                    });
                });
            }
            return primary;
        });
        self.data.availableSecondary = ko.computed(function() {
            var secondary = [];
            if (self.data.newFailureDomain()) {
                $.each(self.data.allStorageRouters(), function(index, storageRouter) {
                    secondary.push({guid: storageRouter.guid(),
                                    name: storageRouter.name(),
                                    disabled: storageRouter.secondaryFailureDomain() === undefined ? false : true})
                });
            } else {
                $.each(self.data.allStorageRouters(), function(index, storageRouter) {
                    secondary.push({guid: storageRouter.guid(),
                                    name: storageRouter.name(),
                                    disabled: false});
                    if (storageRouter.secondaryFailureDomain() !== undefined && storageRouter.secondaryFailureDomain().guid() !== self.data.failureDomain().guid()) {
                        secondary[index].disabled = true;
                    }
                    $.each(self.data.primaryStorageRouters(), function(j_index, srInfo) {
                        if (srInfo.guid === storageRouter.guid()) {
                            secondary[index].disabled = true;
                        }
                    });
                });
            }
            return secondary;
        });
        self.canContinue = ko.computed(function() {
            var value = true, reasons = [], fields = [];
            $.each(self.data.failureDomains(), function(index, failureDomain) {
                if (failureDomain !== undefined && failureDomain.name() !== undefined && failureDomain.name().toLowerCase() === self.data.name()) {
                    value = false;
                    reasons.push($.t('ovs:wizards.add_edit_failure_domains.duplicate_name'));
                }
            });
            if (self.data.primaryStorageRouters().length === 0) {
                value = false;
                reasons.push($.t('ovs:wizards.add_edit_failure_domains.missing_primary'));
            }
            if (!self.data.name.valid()) {
                value = false;
                reasons.push($.t('ovs:wizards.add_edit_failure_domains.invalid_name'));
            }
            return { value: value, reasons: reasons, fields: fields };
        });

        // Subscriptions
        self.data.failureDomain.subscribe(function(failureDomain) {
            var primary = [], secondary = [];
            self.data.city('');
            self.data.name('');
            self.data.address('');
            self.data.country('');
            if (failureDomain !== undefined && !self.data.newFailureDomain()) {
                self.data.city(failureDomain.city());
                self.data.name(failureDomain.name());
                self.data.address(failureDomain.address());
                self.data.country(failureDomain.country());
                $.each(self.data.allStorageRouters(), function(index, storageRouter) {
                    if (storageRouter.primaryFailureDomain() !== undefined && storageRouter.primaryFailureDomain().guid() === self.data.failureDomain().guid()) {
                        var newEntry = {guid: storageRouter.guid(),
                                        name: storageRouter.name(),
                                        disabled: false};
                        primary.push(newEntry);
                    }
                    if (storageRouter.secondaryFailureDomain() !== undefined && storageRouter.secondaryFailureDomain().guid() === self.data.failureDomain().guid()) {
                        var newEntry = {guid: storageRouter.guid(),
                                        name: storageRouter.name(),
                                        disabled: false};
                        secondary.push(newEntry);
                    }
                });
            }
            self.data.primaryStorageRouters(primary);
            self.data.secondaryStorageRouters(secondary);
        });
        self.data.newFailureDomain.subscribe(function(newValue) {
            self.data.primaryStorageRouters([]);
            self.data.secondaryStorageRouters([]);
            if (newValue === true) {
                self.data.city('');
                self.data.name('');
                self.data.address('');
                self.data.country('');
            }
            else {
                self.data.city(self.data.failureDomain().city());
                self.data.name(self.data.failureDomain().name());
                self.data.address(self.data.failureDomain().address());
                self.data.country(self.data.failureDomain().country());
                $.each(self.data.allStorageRouters(), function(index, storageRouter) {
                    if (storageRouter.primaryFailureDomain() !== undefined && storageRouter.primaryFailureDomain().guid() === self.data.failureDomain().guid()) {
                        var newEntry = {guid: storageRouter.guid(),
                                        name: storageRouter.name(),
                                        disabled: false};
                        self.data.primaryStorageRouters.push(newEntry);
                    }
                    if (storageRouter.secondaryFailureDomain() !== undefined && storageRouter.secondaryFailureDomain().guid() === self.data.failureDomain().guid()) {
                        var newEntry = {guid: storageRouter.guid(),
                                        name: storageRouter.name(),
                                        disabled: false};
                        self.data.secondaryStorageRouters.push(newEntry);
                    }
                });
            }
        })

        // Functions
        self.activate = function() {
            var calls = [
                $.Deferred(function(failureDomainHandle) {
                    var options = {
                        sort: 'name',
                        contents: ''
                    };
                    api.get('failure_domain', { queryparams: options })
                        .done(function(data) {
                            var guids = [], fdData = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                fdData[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.data.failureDomains,
                                function(guid) {
                                    return new FailureDomain(guid);
                                }, 'guid'
                            );
                            $.each(self.data.failureDomains(), function(index, item) {
                                if (fdData.hasOwnProperty(item.guid())) {
                                    item.fillData(fdData[item.guid()]);
                                }
                            });
                        });
                    failureDomainHandle.resolve();
                }).promise(),
                $.Deferred(function(storagerouterHandle) {
                    var options = {
                        sort: 'name',
                        contents: '_relations'
                    };
                    api.get('storagerouters', { queryparams: options })
                        .done(function(data) {
                            var guids = [], srData = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                srData[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.data.allStorageRouters,
                                function(guid) {
                                    return new StorageRouter(guid);
                                }, 'guid'
                            );
                            $.each(self.data.allStorageRouters(), function(index, item) {
                                if (srData.hasOwnProperty(item.guid())) {
                                    item.fillData(srData[item.guid()]);
                                }
                            });
                        });
                    storagerouterHandle.resolve();
                }).promise()
            ];
            $.when.apply($, calls)
                .done(function() {
                    self.loaded(true);
                })
                .fail(function() {
                    self.loaded(undefined);
                });
        }
        self.finish = function() {
            return $.Deferred(function(deferred) {
                generic.alertInfo(
                    $.t('ovs:wizards.add_edit_failure_domains.started'),
                    $.t('ovs:wizards.add_edit_failure_domains.started_message', { what: self.data.newFailureDomain() ? "Adding" : "Editing", which: self.data.name() })
                );
                var primaryGuids = [], secondaryGuids = [];
                $.each(self.data.primaryStorageRouters(), function(index, srInfo) {
                    primaryGuids.push(srInfo.guid);
                });
                $.each(self.data.secondaryStorageRouters(), function(index, srInfo) {
                    secondaryGuids.push(srInfo.guid);
                });
                if (self.data.newFailureDomain()) {
                    api.post('failure_domain', {
                        data: {
                            city: self.data.city(),
                            name: self.data.name(),
                            address: self.data.address(),
                            country: self.data.country(),
                            primary: primaryGuids,
                            secondary: secondaryGuids
                        }
                    })
                    .then(shared.tasks.wait)
                    .done(function() {
                        generic.alertSuccess(
                            $.t('ovs:wizards.add_edit_failure_domains.completed'),
                            $.t('ovs:wizards.add_edit_failure_domains.success', { what: self.data.newFailureDomain() ? "Added" : "Edited", which: self.data.name() })
                        );
                    })
                    .fail(function(error) {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.add_edit_failure_domains.failed', {
                                what: self.data.newFailureDomain() ? "Adding" : "Editing",
                                which: self.data.name()
                            })
                        );
                    })
                } else {
                    api.patch('failure_domain/' + self.data.failureDomain().guid(), {
                        queryparams: {
                            contents: '',
                            city: self.data.city(),
                            name: self.data.name(),
                            address: self.data.address(),
                            country: self.data.country(),
                            primary: primaryGuids,
                            secondary: secondaryGuids
                        }
                    })
                    .then(shared.tasks.wait)
                    .done(function() {
                        generic.alertSuccess(
                            $.t('ovs:wizards.add_edit_failure_domains.completed'),
                            $.t('ovs:wizards.add_edit_failure_domains.success', { what: self.data.newFailureDomain() ? "Added" : "Edited", which: self.data.name() })
                        );
                    })
                    .fail(function(error) {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.add_edit_failure_domains.failed', {
                                what: self.data.newFailureDomain() ? "Adding" : "Editing",
                                which: self.data.name()
                            })
                        );
                    })
                }
                deferred.resolve();
            }).promise();
        };
    };
});
