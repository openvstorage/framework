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
    'ovs/api', 'ovs/shared', 'ovs/generic',
    './data'
], function($, ko, api, shared, generic, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data   = data;
        self.shared = shared;

        // Computed
        self.canContinue = ko.computed(function() {
            return { value: true, reasons: [], fields: [] };
        });

        self.finish = function() {
            return $.Deferred(function(deferred) {
                generic.alertInfo(
                    $.t('ovs:wizards.linkhosts.gather.started'),
                    $.t('ovs:wizards.linkhosts.gather.started_msg')
                );
                $.each(self.data.pmachinemap(), function(pmachineGuid, pmachine) {
                    var action = undefined;
                    if (pmachine.originalMgmtCenterGuid() === null && pmachine.mgmtCenter() !== undefined) {
                        action = '/configure_host'
                    } else if (pmachine.originalMgmtCenterGuid() !== null && pmachine.mgmtCenter() === undefined) {
                        action = '/unconfigure_host'
                    }
                    if (pmachine.configure() === true && action !== undefined && !(action === '/unconfigure_host' && pmachine.isConfigured() === false)) {
                        api.post('pmachines/' + pmachine.guid() + action, {
                            data: {
                                mgmtcenter_guid: pmachine.mgmtCenter() === undefined ? null : pmachine.mgmtCenter().guid()
                            }
                        })
                        .then(shared.tasks.wait)
                        .done(function() {
                            generic.alertSuccess(
                                $.t('ovs:wizards.linkhosts.gather.completed', { which: (action === '/configure_host' ? 'Configure' : 'Unconfigure')}),
                                $.t('ovs:wizards.linkhosts.gather.success', { which: (action === '/configure_host' ? 'configured' : 'unconfigured'), what: pmachine.name() })
                            );
                        })
                        .fail(function(error) {
                            generic.alertError(
                                $.t('ovs:generic.error'),
                                $.t('ovs:wizards.linkhosts.gather.failed', {
                                    which: (action === '/configure_host' ? 'Configuring' : 'Unconfiguring'),
                                    what: pmachine.name(),
                                    why: error
                                })
                            );
                        })
                    } else if (action !== undefined) {
                        api.patch('pmachines/' + pmachine.guid(), {
                            data: {
                                mgmtcenter_guid: pmachine.mgmtCenter() === undefined ? null : pmachine.mgmtCenter().guid()
                            },
                            queryparams: { contents: 'mgmtcenter' }
                        })
                        .done(function() {
                            generic.alertSuccess(
                                $.t('ovs:wizards.linkhosts.gather.completed', { which: (action === '/configure_host' ? 'Link' : 'Unlink')}),
                                $.t('ovs:wizards.linkhosts.gather.success', { which: (action === '/configure_host' ? 'linked' : 'unlinked'), what: pmachine.name() })
                            );
                        })
                        .fail(function(error) {
                            generic.alertError(
                                $.t('ovs:generic.error'),
                                $.t('ovs:wizards.linkhosts.gather.failed', {
                                    which: (action === '/configure_host' ? 'Linking' : 'Unlinking'),
                                    what: pmachine.name(),
                                    why: error
                                })
                            );
                        })
                    }
                    pmachine.configure(true); //Set configure/unconfigure flag always on true
                    pmachine.originalMgmtCenterGuid(pmachine.mgmtCenter() === undefined ? null : pmachine.mgmtCenter().guid());
                });
                deferred.resolve();
            }).promise();
        };
    };
});
