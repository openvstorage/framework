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
/*global define, window, require */
define([
    'jquery', 'plugins/router', 'durandal/system', 'durandal/activator', 'bootstrap', 'i18next',
    'ovs/shared', 'ovs/routing', 'ovs/messaging', 'ovs/generic', 'ovs/tasks',
    'ovs/authentication', 'ovs/plugins/cssloader', 'ovs/services/notifications', 'ovs/services/pluginloader', 'ovs/services/cookie',
    'viewmodels/services/user', 'viewmodels/services/misc'
], function ($, router, system, activator, bootstrap, i18n,
             shared, routing, messaging, generic, tasks,
             authentication, cssLoader, notifications, pluginLoader, cookieService,
             userService, miscService) {
    "use strict";
    // Initially load in all routes
    router.map(routing.mainRoutes)
        .buildNavigationModel()
        .mapUnknownRoutes('viewmodels/404');

    return function () {
        var self = this;

        self._translate = function () {
            return $.when().then(function () {
                i18n.setLng(self.shared.language, function () {
                    $('html').i18n(); // Force retranslation of complete UI
                });
            })
        };

        self.shared = shared;
        self.router = router;
        self.compositionComplete = function () {
            return miscService.branding()
                .then(function (brandings) {
                    var i, brand, css;
                    for (i = 0; i < brandings.length; i += 1) {
                        brand = brandings[i];
                        if (brand.is_default === true) {
                            css = brand.css;
                        }
                    }
                    if (css !== undefined) {
                        cssLoader.removeModuleCss();
                        cssLoader.loadCss('css/' + css);
                    }
                });
        };
        self.activate = function () {
            // Setup shared with all instances
            self.shared.messaging = messaging;
            self.shared.authentication = authentication;
            self.shared.tasks = tasks;
            self.shared.routing = routing;

            // Register some callbacks
            authentication.addLogInCallback(function() {  // See if messaging works
                return messaging.start.call(messaging)
            });
            authentication.addLogInCallback(function () {  // Retrieve the current user details
                return $.when().then(function() {
                    return miscService.metadata()
                        .then(function (metadata) {
                                if (!metadata.authenticated) {
                                    // This shouldn't be the case, but is checked anyway.
                                    self.shared.authentication.logout();
                                    throw new Error('User was not logged in. Logging out')
                                }
                                self.shared.authentication.metadata = metadata.authentication_metadata;
                                self.shared.user.username(metadata.username);
                                self.shared.user.guid(metadata.userguid);
                                self.shared.user.roles(metadata.roles);
                                self.shared.releaseName = metadata.release.name;
                                return self.shared.user.guid()
                            })
                        })
                        .then(userService.fetchUser)
                        .then(function (data) {
                            self.shared.language = data.language;
                        })
                        .then(self._translate)
            });
            authentication.addLogInCallback(function () {  // Handle event type messages
                self.shared.messaging.subscribe.call(messaging, 'EVENT', notifications.handleEvent);
            });
            authentication.addLogOutCallback(function () {
                self.shared.language = self.shared.defaultLanguage;
                return self._translate();
            });
            var token = window.localStorage.getItem('accesstoken'), state, expectedState;
            if (token === null) {
                token = cookieService.getCookie('accesstoken');
                if (token !== null) {
                    state = cookieService.getCookie('state');
                    expectedState = window.localStorage.getItem('state');
                    if (state === null || state !== expectedState) {
                        token = null;
                    } else {
                        window.localStorage.setItem('accesstoken', token);
                    }
                    cookieService.removeCookie('accesstoken');
                    cookieService.removeCookie('state');
                }
            }
            if (token !== null) {
                self.shared.authentication.accessToken(token);
            }
            return $.when().then(function() {
                    return miscService.metadata()
                        // @todo handle failures - do a promise retry and swallow error on x'th retry
                        .then(function (metadata) {
                            var metadataPromises = [], backendsActive = false;
                            // Load plugin views and viewmodels
                            $.each(metadata.plugins, function (plugin, types) {
                                if (types.contains('gui')) {
                                    // i18n works with callbacks. Make it a promise for loading it all in concurrently
                                    var moduleHandler = $.Deferred(function (translationDeferred) {
                                        i18n.loadNamespace(plugin, function () {
                                            translationDeferred.resolve();
                                        });
                                    }).promise();
                                    metadataPromises.push(pluginLoader.load_hooks(plugin).then(function(){
                                        // Plugins are loaded here
                                        $.extend(self.shared.hooks.wizards, pluginLoader.wizards);
                                        $.extend(self.shared.hooks.dashboards, pluginLoader.dashboards);
                                        $.extend(self.shared.hooks.pages, pluginLoader.pages);
                                    }));
                                    metadataPromises.push(moduleHandler);
                                }
                                if (types.contains('backend') && !backendsActive) {
                                    // Enable backend view
                                    routing.siteRoutes.push({
                                        route: 'backends',
                                        moduleId: 'backend/backends',
                                        title: $.t('ovs:backends.title'),
                                        titlecode: 'ovs:backends.title',
                                        nav: true,
                                        main: true
                                    });
                                    backendsActive = true;
                                }
                            });
                            self.shared.authentication.metadata = metadata.authentication_metadata;
                            if (metadata.authenticated) {
                                metadataPromises.push(self.shared.authentication.dispatch(true));
                            }
                            // Wait for all promises to resolve
                            return $.when.apply($, metadataPromises);
                        })
                .then(router.activate)
            });
        };
    };
});
