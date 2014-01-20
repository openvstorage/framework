// license see http://www.openvstorage.com/licenses/opensource/
/*global requirejs, define */
requirejs.config({
    paths: {
        'text'       : '../lib/require/text',
        'durandal'   : '../lib/durandal/js',
        'plugins'    : '../lib/durandal/js/plugins',
        'transitions': '../lib/durandal/js/transitions',
        'knockout'   : '../lib/knockout/knockout-3.0.0',
        'bootstrap'  : '../lib/bootstrap/js/bootstrap',
        'jquery'     : '../lib/jquery/jquery-1.9.1',
        'jqp'        : '../lib/jquery-plugins/js',
        'd3'         : '../lib/d3/d3.v3.min',
        'ovs'        : '../lib/ovs',
        'i18next'    : '../lib/i18next/i18next.amd.withJQuery-1.7.1'
    },
    shim: {
        'bootstrap': {
            deps   : ['jquery'],
            exports: 'jQuery'
        },
        'jqp/pnotify': {
            deps   : ['jquery'],
            exports: 'jQuery'
        },
        'd3': {
            exports: 'd3'
        }
    },
    urlArgs: "version=0.0.0b0",
    waitSeconds: 300
});

define([
    'durandal/system', 'durandal/app', 'durandal/viewLocator', 'durandal/binder', 'jquery', 'i18next',
    'ovs/shared',
    'ovs/extensions/knockout-helpers', 'ovs/extensions/knockout-bindinghandlers', 'ovs/extensions/knockout-extensions', 'ovs/extensions/knockout-extenders',
    'bootstrap'
],  function(system, app, viewLocator, binder, $, i18n, shared) {
    "use strict";
    system.debug(true);

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

    i18n.init(i18nOptions, function() {
        app.title = $.t('ovs:title');
        app.configurePlugins({
            router: true,
            dialog: true,
            widget: true
        });
        app.configurePlugins({
            widget: {
                kinds: ['pager', 'lazyloader', 'lazylist', 'footer', 'dropdown']
            }
        });
        app.start().then(function() {
            viewLocator.useConvention();
            binder.binding = function(obj, view) {
                $(view).i18n();
            };
        });
        app.setRoot('viewmodels/shell');
    });
});
