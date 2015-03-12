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
/*global define, window, require */
define([
    'jquery', 'plugins/router', 'bootstrap', 'i18next',
    'ovs/shared', 'ovs/routing', 'ovs/messaging', 'ovs/generic', 'ovs/tasks',
    'ovs/authentication', 'ovs/api', 'ovs/plugins/cssloader', 'ovs/notifications'
], function($, router, bootstrap, i18n, shared, routing, Messaging, generic, Tasks, Authentication, api, cssLoader, notifications) {
    "use strict";
    router.map(routing.mainRoutes)
          .buildNavigationModel()
          .mapUnknownRoutes('viewmodels/404');

    return function() {
        var self = this;

        self._translate = function() {
            return $.Deferred(function(deferred) {
                i18n.setLng(self.shared.language, function() {
                    $('html').i18n(); // Force retranslation of complete UI
                    deferred.resolve();
                });
            }).promise();
        };

        self.shared = shared;
        self.router = router;
        self.compositionComplete = function() {
            return api.get('branding')
                .then(function(brandings) {
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
        self.activate = function() {
            self.shared.messaging      = new Messaging();
            self.shared.authentication = new Authentication();
            self.shared.tasks          = new Tasks();
            self.shared.routing        = routing;

            self.shared.authentication.onLoggedIn.push(self.shared.messaging.start);
            self.shared.authentication.onLoggedIn.push(function() {
                return $.Deferred(function(deferred) {
                    api.get('')
                        .then(function(metadata) {
                            return $.Deferred(function(mdDeferred) {
                                self.shared.user.username(undefined);
                                self.shared.user.guid(undefined);
                                self.shared.user.roles([]);
                                if (!metadata.authenticated) {
                                    // This shouldn't be the case, but is checked anyway.
                                    self.shared.authentication.logout();
                                    return mdDeferred.reject();
                                }
                                self.shared.user.username(metadata.username);
                                self.shared.user.guid(metadata.userguid);
                                self.shared.user.roles(metadata.roles);
                                mdDeferred.resolve();
                            }).promise();
                        })
                        .then(function() {
                            return api.get('users/' + self.shared.user.guid());
                        })
                        .then(function(data) {
                            self.shared.language = data.language;
                        })
                        .then(self._translate)
                        .always(deferred.resolve);
                }).promise();
            });
            self.shared.authentication.onLoggedIn.push(function() {
                self.shared.messaging.subscribe('EVENT', notifications.handleEvent);
            });
            self.shared.authentication.onLoggedOut.push(function() {
                self.shared.language = self.shared.defaultLanguage;
                return self._translate();
            });
            var token = window.localStorage.getItem('accesstoken');
            if (token !== null) {
                self.shared.authentication.accessToken(token);
            }
            return $.Deferred(function(activateDeferred) {
                $.Deferred(function(metadataCheckDeferred) {
                    api.get('')
                        .done(function(metadata) {
                            var metadataHandlers = [], backendsActive = false;
                            $.each(metadata.plugins, function(plugin, types) {
                                if ($.inArray('gui', types) !== -1) {
                                    var moduleHandler = $.Deferred(function(translationDeferred) {
                                        i18n.loadNamespace(plugin, function () {
                                            translationDeferred.resolve();
                                        });
                                    }).promise();
                                    moduleHandler.then(function() {
                                        return $.Deferred(function(moduleDeferred) {
                                            require(['ovs/routes/' + plugin], function(routes) {
                                                routing.extraRoutes.push(routes.routes);
                                                moduleDeferred.resolve();
                                            });
                                        }).promise();
                                    });
                                    metadataHandlers.push(moduleHandler);
                                }
                                if ($.inArray('backend', types) !== -1 && !backendsActive) {
                                    routing.siteRoutes.push({
                                        route: 'backends',
                                        moduleId: 'backends',
                                        title: $.t('ovs:backends.title'),
                                        titlecode: 'ovs:backends.title',
                                        nav: true,
                                        main: true
                                    });
                                    backendsActive = true;
                                }
                            });
                            if (metadata.authenticated) {
                                metadataHandlers.push(self.shared.authentication.dispatch(true));
                            }
                            $.when.apply($, metadataHandlers).always(metadataCheckDeferred.resolve);
                        })
                        .fail(metadataCheckDeferred.resolve);
                }).promise()
                    .then(router.activate)
                    .always(activateDeferred.resolve);
            }).promise();
        };
    };
});
