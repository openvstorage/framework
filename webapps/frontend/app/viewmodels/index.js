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
/*global define, window */
define([
    'plugins/router', 'plugins/dialog', 'jqp/pnotify',
    'ovs/shared', 'viewmodels/wizards/changepassword/index'
], function(router, dialog, $, shared, ChangePasswordWizard) {
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
        if (instance !== undefined && instance.hasOwnProperty('guard')) {
            if (instance.guard.authenticated === true) {
                if (instance.shared.authentication.validate()) {
                    return true;
                }
                window.localStorage.setItem('referrer', instruction.fragment);
                return instruction.params[0] + '/login';
            }
        }
        return true;
    };
    childRouter.mapUnknownRoutes('../404');

    return {
        shared: shared,
        router: childRouter,
        changePassword: function() {
            dialog.show(new ChangePasswordWizard({
                modal: true
            }));
        },
        activate: function(mode) {
            var self = this;
            // Config
            self.shared.mode(mode);

            // Notifications
            $.pnotify.defaults.history = false;
            $.pnotify.defaults.styling = "bootstrap";

            // Cache node ips
            $.ajax('/api/internal/generic/0/?timestamp=' + (new Date().getTime()), {
                    type: 'GET',
                    contentType: 'application/jsonp',
                    timeout: 5000
                })
                .done(function(nodes) {
                    shared.nodes = nodes.storageappliance_ips;
                    window.localStorage.setItem('nodes', JSON.stringify(shared.nodes));
                });
        }
    };
});
