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
    'ovs/api', 'ovs/generic', 'ovs/shared',
    './data'
], function($, ko, api, generic, shared, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data   = data;
        self.shared = shared;

        // Observables
        self.componentFwk              = ko.observable();
        self.componentPlugins          = ko.observableArray([]);
        self.componentSd               = ko.observable();
        self.loadingInformationFailure = ko.observable(false);
        self.loadingInformation        = ko.observable(false);
        self.updateInformation         = ko.observableArray([]);

        // Handles
        self.loadUpdateInformation = undefined;

        // Computed
        self.canContinue = ko.computed(function() {
            var reasons = [], anyPluginChecked = false;
            if (self.loadingInformationFailure() === true) {
                reasons.push($.t('ovs:wizards.update.loading_information_failure'));
            } else {
                if (self.componentFwk() !== undefined && self.componentFwk().checked() === true) {
                    anyPluginChecked = true;
                    if (self.componentFwk().prerequisites.length > 0) {
                        reasons.push($.t('ovs:wizards.update.prerequisites_unmet'))
                    }
                }
                if (self.componentSd() !== undefined && self.componentSd().checked() === true) {
                    anyPluginChecked = true;
                    if (self.componentSd().prerequisites.length > 0) {
                        reasons.push($.t('ovs:wizards.update.prerequisites_unmet'))
                    }
                }
                $.each(self.componentPlugins(), function(index, plugin) {
                    if (plugin.checked() === true) {
                        anyPluginChecked = true;
                        if (plugin.prerequisites.length > 0) {
                            reasons.push($.t('ovs:wizards.update.prerequisites_unmet'))
                        }
                    }
                });
                if (anyPluginChecked === false) {
                    reasons.push($.t('ovs:wizards.update.component_choose'));
                }
            }
            reasons = reasons.getUnqiue();
            return { value: reasons.length === 0, reasons: reasons, fields: [] };
        });
        self.frameworkMessages = ko.computed(function() {
            var downtimes = [], prerequisites = [];
            if (self.componentFwk() !== undefined) {
                $.each(self.componentFwk().downtime, function (index, downtime) {
                    if (downtime[1] === null) {
                        downtimes.push($.t('ovs:wizards.update.downtime.' + downtime[0]))
                    } else {
                        downtimes.push($.t('ovs:wizards.update.downtime.' + downtime[0]) + ': ' + downtime[1])
                    }
                });
                $.each(self.componentFwk().prerequisites, function (index, prereq) {
                    if (prereq[1] === null) {
                        prerequisites.push($.t('ovs:wizards.update.prerequisites.' + prereq[0]))
                    } else {
                        prerequisites.push($.t('ovs:wizards.update.prerequisites.' + prereq[0]) + ': ' + prereq[1])
                    }
                });
            }
            downtimes = downtimes.getUnqiue();
            prerequisites = prerequisites.getUnqiue();
            downtimes.sort(function(downtime1, downtime2) {
                return downtime1 < downtime2 ? -1 : 1;
            });
            prerequisites.sort(function(prerequisite1, prerequisite2) {
                return prerequisite1 < prerequisite2 ? -1 : 1;
            });
            return {downtimes: downtimes,
                    prerequisites: prerequisites};
        });
        self.storagedriverMessages = ko.computed(function() {
            var downtimes = [], prerequisites = [];
            if (self.componentSd() !== undefined) {
                $.each(self.componentSd().downtime, function (index, downtime) {
                    if (downtime[1] === null) {
                        downtimes.push($.t('ovs:wizards.update.downtime.' + downtime[0]))
                    } else {
                        downtimes.push($.t('ovs:wizards.update.downtime.' + downtime[0]) + ': ' + downtime[1])
                    }
                });
                $.each(self.componentSd().prerequisites, function (index, prereq) {
                    if (prereq[1] === null) {
                        prerequisites.push($.t('ovs:wizards.update.prerequisites.' + prereq[0]))
                    } else {
                        prerequisites.push($.t('ovs:wizards.update.prerequisites.' + prereq[0]) + ': ' + prereq[1])
                    }
                });
            }
            downtimes = downtimes.getUnqiue();
            prerequisites = prerequisites.getUnqiue()
            downtimes.sort(function(downtime1, downtime2) {
                return downtime1 < downtime2 ? -1 : 1;
            });
            prerequisites.sort(function(prerequisite1, prerequisite2) {
                return prerequisite1 < prerequisite2 ? -1 : 1;
            });
            return {downtimes: downtimes,
                    prerequisites: prerequisites};
        });
        self.pluginMessages = ko.computed(function() {
            var messages = {};
            $.each(self.componentPlugins(), function(index, plugin) {
                var downtimes = [], prerequisites = [];
                $.each(plugin.downtime, function (index, downtime) {
                    if (downtime[1] === null) {
                        downtimes.push($.t(plugin.name + ':wizards.update.downtime.' + downtime[0]))
                    } else {
                        downtimes.push($.t(plugin.name + ':wizards.update.downtime.' + downtime[0]) + ': ' + downtime[1])
                    }
                });
                $.each(plugin.prerequisites, function (index, prereq) {
                    if (prereq[1] === null) {
                        prerequisites.push($.t(plugin.name + ':wizards.update.prerequisites.' + prereq[0]))
                    } else {
                        prerequisites.push($.t(plugin.name + ':wizards.update.prerequisites.' + prereq[0]) + ': ' + prereq[1])
                    }
                });
                downtimes = downtimes.getUnqiue();
                prerequisites = prerequisites.getUnqiue();
                downtimes.sort(function(downtime1, downtime2) {
                    return downtime1 < downtime2 ? -1 : 1;
                });
                prerequisites.sort(function(prerequisite1, prerequisite2) {
                    return prerequisite1 < prerequisite2 ? -1 : 1;
                });
                messages[plugin.name] = {downtimes: downtimes,
                                         prerequisites: prerequisites};
            });
            return messages;
        });

        // Functions
        self.finish = function() {
            return $.Deferred(function(deferred) {
                var components = [];
                if (self.componentFwk() !== undefined && self.componentFwk().checked() === true) {
                    components.push('framework');
                }
                if (self.componentSd() !== undefined && self.componentSd().checked() === true) {
                    components.push('storagedriver');
                }
                $.each(self.componentPlugins(), function(index, plugin) {
                    if (plugin.checked() === true) {
                        components.push(plugin.name);
                    }
                });
                api.post('storagerouters/' + self.data.storageRouter().guid() + '/update_components', { queryparams: { components: JSON.stringify(components) } })
                    .then(function(taskID) {
                        generic.alertInfo(
                            $.t('ovs:wizards.update.started'),
                            $.t('ovs:wizards.update.started_msg')
                        );
                        deferred.resolve();
                        return taskID;
                    })
                    .then(self.shared.tasks.wait)
                    .fail(function(error) {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.update.failure', { why: generic.extractErrorMessage(error) })
                        );
                        deferred.reject();
                    });
            }).promise();
        };

        // Durandal
        self.activate = function() {
            self.loadingInformation(true);
            if (generic.xhrCompleted(self.loadUpdateInformation)) {
                self.loadUpdateInformation = api.get('storagerouters/' + self.data.storageRouter().guid() + '/get_update_information')
                    .then(self.shared.tasks.wait)
                    .done(function(data) {
                        $.each(data, function(component, info) {
                            info.name = component;
                            if (component === 'framework') {
                                info.checked = ko.observable(true);
                                self.componentFwk(info);
                            } else if (component === 'storagedriver') {
                                info.checked = ko.observable(false);
                                self.componentSd(info);
                            } else {
                                info.checked = ko.observable(false);
                                self.componentPlugins.push(info);
                            }
                        });
                        // Attempt to have 1 component 'checked'
                        if (self.componentFwk() === undefined) {
                            if (self.componentPlugins().length === 0) {
                                self.componentSd().checked(true);
                            } else {
                                if (self.componentSd() === undefined && self.componentPlugins().length === 1) {
                                    self.componentPlugins()[0].checked(true);
                                }
                            }
                        }
                    })
                    .fail(function() {
                        self.loadingInformationFailure(true);
                    })
                    .always(function () {
                        self.loadingInformation(false);
                    });
            }
        };
    };
});
