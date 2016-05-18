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
