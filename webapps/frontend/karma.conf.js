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
/*global module */
module.exports = function(config) {
    "use strict";
    config.set({
        basePath: '',
        frameworks: ['jasmine', 'requirejs'],
        files: [
            'tests/main.js',
            { pattern: 'lib/**/*.js',   included: false },
            { pattern: 'app/**/*.js',   included: false },
            { pattern: 'tests/**/*.js', included: false }
        ],
        exclude: [
            'app/main.js',
            'tests/coverage/**/*.js'
        ],
        preprocessors: {
            '**/lib/ovs/*.js'       : ['coverage'],
            '**/app/viewmodels/*.js': ['coverage'],
            '**/app/widgets/**/*.js': ['coverage'],
            '**/tests/**/*.js'      : ['coverage']
        },
        reporters: ['progress', 'coverage', 'junit'],
        port: 9876,
        colors: true,
        logLevel: config.LOG_INFO,
        autoWatch: true,
        browsers: ['PhantomJS'],
        captureTimeout: 60000,
        singleRun: true,
        coverageReporter: {
            type: 'cobertura',
            dir: 'tests/coverage/',
            file: 'coverage.txt'
        },
        junitReporter: {
            type: 'cobertura',
            outputFile: 'test-results.xml'
        }
    });
};
