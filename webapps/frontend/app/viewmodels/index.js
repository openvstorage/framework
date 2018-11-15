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
/*global define, window */
define([
    'plugins/router', 'plugins/dialog', 'jqp/pnotify',
    'ovs/shared', 'ovs/generic', 'ovs/api',
    'ovs/services/authentication',
    'jqp/timeago'
], function(router, dialog, $,
            shared, generic, api,
            authentication) {
    "use strict";
    var modeInstruction = router.activeInstruction().params[0];
    var mode = modeInstruction|| shared.modes.FULL;
    // I suspect that there is a bug within the Durandal router. This value is sometimes undefined which gets passed into
    // route building. All routes are then prefixed with '#undefined'. Adding the failsafe to default to full is the work-around
    // The actual cause is swept under the mat though (mainly because it has not been reproducable so far).
    if (!modeInstruction) {
        console.warn('Router received an invalid mode instruction. It was reverted to full. Instruction was {0}'.format([modeInstruction]))
    }
    shared.routing.buildSiteRoutes(mode);
    var childRouter = router.createChildRouter()
                        .makeRelative({
                            moduleId: 'viewmodels/site',
                            route: ':mode',
                            fromParent: true
                        })
                        .map(shared.routing.siteRoutes)
                        .buildNavigationModel();
    childRouter.guardRoute = function(instance, instruction) {
        var state, metadata;
        if (instance !== undefined && instance.hasOwnProperty('guard')) {
            if (instance.guard.authenticated === true) {
                if (!authentication.loggedIn()) {
                    window.localStorage.setItem('referrer', instruction.fragment);
                    state = window.localStorage.getItem('state');
                    if (state === null && authentication.metadata.mode === 'remote') {
                        metadata = authentication.metadata;
                        state = generic.getTimestamp() + '_' + Math.random().toString().substr(2, 10);
                        window.localStorage.setItem('state', state);
                        return metadata.authorize_uri +
                            '?response_type=code' +
                            '&client_id=' + encodeURIComponent(metadata.client_id) +
                            '&redirect_uri=' + encodeURIComponent('https://' + window.location.host + '/api/oauth2/redirect/') +
                            '&state=' + encodeURIComponent(state) +
                            '&scope=' + encodeURIComponent(metadata.scope);
                    }
                    return instruction.params[0] + '/login';
                }
            }
        }
        window.localStorage.removeItem('state');
        return true;
    };
    childRouter.mapUnknownRoutes('../404');

    function IndexViewModel() {
        this.shared = shared;
        this.router = childRouter;
        this.authentication = authentication;
    }
    IndexViewModel.prototype = {
        // Activate is called by the router with the params in the route
        // The parent route in routing.js is { route: ':mode*details', moduleId: 'viewmodels/index',    nav: false }
        // Which makes the first activate param the mode, the second the details (could be anything)
        activate: function(mode) {
            // Config
            shared.mode(mode);
            // Notifications
            $.pnotify.defaults.history = false;
            $.pnotify.defaults.styling = "bootstrap";

            // Fetch main API metadata
            api.get('', { timeout: 5000 })
                .done(function(metadata) {
                    api.setNodes(metadata.storagerouter_ips);
                    shared.identification(metadata.identification);
                });
        }
    };
    return IndexViewModel
});
