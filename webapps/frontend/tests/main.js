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
/*global requirejs, window */
var tests = [], file;
Object.prototype.invalidValue = 0;
for (file in window.__karma__.files) {
    if (window.__karma__.files.hasOwnProperty(file)) {
        if (/test.+\.js$/.test(file) && !/test.+main\.js$/.test(file)) {
            tests.push(file);
        }
    }
}

requirejs.config({
    // Karma serves files from '/base'
    baseUrl: '/base/app',
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
    urlArgs: "bust=0.1.0.0b",

    deps: tests,
    callback: window.__karma__.start
});
