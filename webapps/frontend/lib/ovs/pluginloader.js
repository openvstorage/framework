// Copyright (C) 2018 iNuron NV
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
define(['jquery', 'knockout',
    'durandal/system', 'durandal/activator',
    'ovs/routing', 'ovs/shared'
], function ($, ko,
             system, activator,
             routing, shared) {
    "use strict";

    var cache = {};
    function ViewCache(){
        var self = this;
    }
    ViewCache.prototype = {
        hasPageCached: function(page, identifier){
            var self = this;
            return page in cache
        }
    };
    function PluginLoader() {
        var self = this;
        self.pages = {};
        self.wizards = {};
        self.dashboards = [];
    }

    var functions = {
        load_hooks: function (plugin) {
            var self = this;
            return $.when().then(function () {
                // shared.pluginData[plugin] = {};   // Add a plugin key to the shared.pluginData value
                // Requirejs works with callbacks. To chain it, wrap it up
                return $.Deferred(function (moduleLoadingDeferred){
                    require(['ovs/hooks/' + plugin], function (hook) { // webapps/frontend/lib/ovs/hooks
                        var systemLoaders = [];
                        routing.extraRoutes.push(hook.routes);
                        routing.routePatches.push(hook.routePatches);
                        $.each(hook.dashboards, function (index, dashboard) {
                            systemLoaders.push(system.acquire('viewmodels/site/' + dashboard)
                                .then(function (module) {
                                    var moduleInstance = new module();
                                    self.dashboards.push({
                                        module: moduleInstance,
                                        activator: activator.create()
                                    });
                                }));
                        });
                        $.each(hook.wizards, function (wizard, moduleName) {
                            if (!shared.hooks.wizards.hasOwnProperty(wizard)) {
                                shared.hooks.wizards[wizard] = [];
                            }
                            systemLoaders.push(system.acquire('viewmodels/wizards/' + wizard + '/' + moduleName)
                                .then(function (module) {
                                    var moduleInstance = new module();
                                    shared.hooks.wizards[wizard].push({
                                        name: moduleName,
                                        module: moduleInstance,
                                        activator: activator.create()
                                    });
                                }));
                        });
                        $.each(hook.pages, function (page, pageInfo) {
                            if (!self.pages.hasOwnProperty(page)) {
                                self.pages[page] = [];
                            }
                            $.each(pageInfo, function (index, info) {
                                var moduleName;
                                if (typeof info === 'string') {
                                    moduleName = info;
                                    info = {type: 'generic', module: moduleName};
                                } else {
                                    moduleName = info.module;
                                }
                                systemLoaders.push(system.acquire('viewmodels/site/' + moduleName)
                                    .then(function (module) {
                                        // Always returns the same instance when item would get activated!
                                        var moduleInstance = new module();
                                        self.pages[page].push({
                                            info: info,
                                            name: moduleName,
                                            module: moduleInstance,
                                            activator: activator.create()
                                        });
                                    }));
                            });
                        });
                        return $.when.apply(self, systemLoaders)
                            // Even when failing to load. Resolve routing
                            .always(moduleLoadingDeferred.resolve)
                });
                }).promise();
            }).then(function(){
                // All loaded up!
                return self;
            })
        },
        add_page: function () {
        },
        get_plugin_pages: function (plugin_name, identifier) {
            var self = this;
            var out = [];
            $.each(self.pages, function (pageType, pages) {
                if (pageType === plugin_name) {
                    $.each(pages, function (index, page) {
                        // Load in the activators for the plugins
                        out.push(page)
                    })
                }
            });
            return out
        },
        activate_page: function (page) {
            page.activator.activateItem(page.module).fail(function (error) {
                console.error(error)
            })
        },
        deactivate_page: function (page) {
            page.activator.deactivateItem(page.module);
        }
    };

    PluginLoader.prototype = $.extend({}, functions);
    return new PluginLoader()
});