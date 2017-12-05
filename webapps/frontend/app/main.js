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
/*global requirejs, define, window */
requirejs.config({
    paths: {  // paths for module names not found under baseUrl (http://requirejs.org/docs/api.html#config-paths)
        'text'                  : '../lib/require/text',
        'durandal'              : '../lib/durandal/js',
        'plugins'               : '../lib/durandal/js/plugins',
        'transitions'           : '../lib/durandal/js/transitions',
        'knockout'              : '../lib/knockout/knockout-3.4.0',
        'knockout-mapping'      : '../lib/knockout-plugins/knockout-mapping-2.4.1',
        'knockout-dictionary'   : '../lib/knockout-plugins/observableDictionary',
        'bootstrap'             : '../lib/bootstrap/js/bootstrap',
        'jquery'                : '../lib/jquery/jquery-3.2.1',
        'jqp'                   : '../lib/jquery-plugins/js',
        'd3'                    : '../lib/d3/d3.v3.min',
        'd3p'                   : '../lib/d3-plugins/js',
        'ovs'                   : '../lib/ovs',
        'i18next'               : '../lib/i18next/i18next.amd.withJQuery-1.7.1'
    },
    shim: {
        'knockout-mapping': {
            deps: ['knockout'],
                exports: 'knockout-mapping'
        },
        'bootstrap': {
            deps   : ['jquery'],
            exports: 'jQuery'
        },
        'jqp/pnotify': {
            deps   : ['jquery'],
            exports: 'jQuery'
        },
        'jqp/timeago': {
            deps   : ['jquery'],
            exports: 'jQuery'
        },
        'd3': {
            exports: 'd3'
        },
        'd3p/slider': {
            deps   : ['d3'],
            exports: 'd3'
        }
    },
    // urlArgs: 'version=0.0.0b0',
    waitSeconds: 300,
    // Configuration dependencies
    deps: ['knockout', 'knockout-mapping'],
    // Configuration callback - executed when all dependencies are loaded
    callback: function (ko, mapping) {
        ko.mapping = mapping;  // Load in the plugin
    }
});

define([
    'durandal/system', 'durandal/app', 'durandal/viewLocator', 'durandal/binder', 'jquery', 'i18next', 'plugins/dialog',
    'ovs/shared',
    'viewmodels/wizards/context',
    'ovs/extensions/knockout-helpers', 'ovs/extensions/knockout-bindinghandlers', 'ovs/extensions/knockout-extensions', 'ovs/extensions/knockout-extenders',
    'bootstrap',
    'knockout-dictionary' // Ko plugins
],  function(system, app, viewLocator, binder, $, i18n, dialog, shared, wizardContext) {
    "use strict";
    system.debug(true);  // To be changed when building production
    shared.defaultLanguage = shared.language = window.navigator.userLanguage || window.navigator.language || 'en-US';
    var i18nOptions = {
        detectFromHeaders: false,
        lng: shared.defaultLanguage,
        fallbackLng: 'en-US',
        ns: 'ovs',
        resGetPath: requirejs.toUrl('/locales/__lng__/__ns__.json'),
        useCookie: false,
        useLocalStorage: false
    };

    if (window.localStorage.hasOwnProperty('nodes') && window.localStorage.nodes !== null) {
        shared.nodes = $.parseJSON(window.localStorage.nodes);
    }

    i18n.init(i18nOptions, function() {
        app.title = $.t('ovs:title');
        app.configurePlugins({
            router: true,
            dialog: true,
            widget: true
        });
        app.configurePlugins({
            widget: {
                kinds: ['pager', 'lazyloader', 'lazylist', 'footer', 'dropdown', 'accessrights', 'numberinput', 'searchbar']
            }
        });
        app.start().then(function() {
            viewLocator.useConvention();  // Map view <-> viewmodel
            binder.binding = function(obj, view) {
                $(view).i18n();
            };
            app.setRoot('viewmodels/shell');
        });
    });

    // Override the default context as the Durandal default context is rather sub awesome with bigger modals
    dialog.addContext('default', wizardContext);
    // Apply modal-dialog for showMessages (missing somehow in Durandal)
    dialog.MessageBox.setDefaults({ "class": "modal-dialog modal-content" });
});
