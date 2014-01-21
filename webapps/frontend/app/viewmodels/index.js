// license see http://www.openvstorage.com/licenses/opensource/
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
                    shared.nodes = nodes.vsa_ips;
                    window.localStorage.setItem('nodes', JSON.stringify(shared.nodes));
                });
        }
    };
});
