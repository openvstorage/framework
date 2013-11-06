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
            'lib/ovs/*.js'       : ['coverage'],
            'app/viewmodels/*.js': ['coverage'],
            'app/widgets/**/*.js': ['coverage'],
            'tests/**/*.js'      : ['coverage']
        },
        reporters: ['progress', 'coverage'],
        port: 9876,
        colors: true,
        logLevel: config.LOG_INFO,
        autoWatch: true,
        browsers: ['PhantomJS'],
        captureTimeout: 60000,
        singleRun: false,
        coverageReporter: {
            type: 'text',
            dir: 'tests/coverage/',
            file: 'coverage.txt'
        }
    });
};
