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
