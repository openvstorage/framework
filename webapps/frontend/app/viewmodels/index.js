// Copyright 2014 iNuron NV
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
/*global define, window */
define([
    'plugins/router', 'plugins/dialog', 'jqp/pnotify',
    'ovs/shared', 'ovs/generic',
    'jqp/timeago'
], function(router, dialog, $, shared, generic) {
    "use strict";
    var mode, childRouter;
    mode = router.activeInstruction().params[0];
    shared.routing.buildSiteRoutes(mode);
    childRouter = router.createChildRouter()
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
                if (!instance.shared.authentication.validate()) {
                    window.localStorage.setItem('referrer', instruction.fragment);
                    state = window.localStorage.getItem('state');
                    if (state === null && instance.shared.authentication.metadata.mode === 'remote') {
                        metadata = instance.shared.authentication.metadata;
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
            if (instance.guard.registered === true && shared.registration().registered === false) {
                if (shared.registration().remaining <= 0) {
                    return instruction.params[0] + '/register';
                } else {
                    window.localStorage.setItem('referrer', instruction.params[0] + '/register');
                }
            }
        }
        window.localStorage.removeItem('state');
        return true;
    };
    childRouter.mapUnknownRoutes('../404');

    return {
        shared: shared,
        router: childRouter,
        activate: function(mode) {
            var self = this;
            // Config
            self.shared.mode(mode);

            // Notifications
            $.pnotify.defaults.history = false;
            $.pnotify.defaults.styling = "bootstrap";

            // Fetch main API metadata
            $.ajax('/api/?timestamp=' + (new Date().getTime()), {
                    type: 'GET',
                    contentType: 'application/json',
                    timeout: 5000,
                    headers: { Accept: 'application/json' }
                })
                .done(function(metadata) {
                    shared.nodes = metadata.storagerouter_ips;
                    shared.identification(metadata.identification);
                    window.localStorage.setItem('nodes', JSON.stringify(shared.nodes));
                });
        }
    };
});
