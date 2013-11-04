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
        'ovs'        : '../lib/ovs'
    },
    shim: {
        'bootstrap': {
            deps   : ['jquery'],
            exports: 'jQuery'
        },
        'jqp/pnotify': {
            deps   : ['jquery'],
            exports: 'jQuery'
        }
    },
    urlArgs: "bust=0.1.0.0b"
});

define([
    'durandal/system', 'durandal/app', 'durandal/viewLocator'
],  function(system, app, viewLocator) {
    "use strict";
    system.debug(true);

    app.title = 'Open vStorage';
    app.configurePlugins({
        router: true,
        dialog: true,
        widget: true
    });
    app.configurePlugins({
        widget: {
            kinds: ['pager']
        }
    });
    app.start().then(function() {
        viewLocator.useConvention();
        app.setRoot('viewmodels/shell');
    });
});