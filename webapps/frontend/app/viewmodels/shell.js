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
    'ovs/shared', 'ovs/routing', 'ovs/generic',
    'ovs/plugins/cssloader', 'ovs/plugins/pluginloader',
    'viewmodels/services/misc'
], function ($, router, system, activator, bootstrap, i18n,
             shared, routing, generic,
             cssLoader, pluginLoader,
             miscService) {
    "use strict";
    // Initially load in all routes
    router.map(routing.mainRoutes)
        .buildNavigationModel()
        .mapUnknownRoutes('viewmodels/404');

    return function () {
        var self = this;

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
