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
/*global define, window */
define([
    'durandal/activator', 'plugins/dialog', 'knockout', 'jquery'
], function(activator, dialog, ko, $) {
    "use strict";
    return function(parent) {
        // Observables
        parent.title       = ko.observable();
        parent.step        = ko.observable(0);
        parent.modal       = ko.observable(false);
        parent.running     = ko.observable(false);
        parent.steps       = ko.observableArray([]);
        parent.loadingNext = ko.observable(false);

        // Deferreds
        parent.closing   = $.Deferred();
        parent.finishing = $.Deferred();

        // Builded variable
        parent.activeStep = activator.create();

        // Computed
        parent.stepsLength = ko.computed(function() {
            return parent.steps().length;
        });
        parent.hasPrevious = ko.computed(function() {
            if (parent.running()) {
                return false;
            }
            return parent.step() > 0;
        });
        parent.hasNext = ko.computed(function() {
            if (parent.running()) {
                return false;
            }
            if (parent.step() < parent.stepsLength() - 1 && parent.stepsLength() > 1) {
                return parent.canContinue().value === true;
            }
            return false;
        });
        parent.hasFinish = ko.computed(function() {
            if (parent.running()) {
                return false;
            }
            if (parent.step() === parent.stepsLength() - 1) {
                return parent.canContinue().value === true;
            }
            return false;
        });
        parent.canContinue = ko.computed(function() {
            var step = parent.steps()[parent.step()];
            if (step !== undefined) {
                return step.canContinue();
            }
            return { value: true, reasons: [], fields: [] };
        });

        // Functions
        parent.next = function() {
            if (parent.step() < parent.stepsLength() ) {
                var step = parent.steps()[parent.step()],
                    chainDeferred = $.Deferred(), chainPromise = chainDeferred.promise();
                parent.loadingNext(true);
                $.Deferred(function(deferred) {
                    chainDeferred.resolve();
                    if (step.hasOwnProperty('preValidate') && step.preValidate && step.preValidate.call) {
                        chainPromise = chainPromise.then(step.preValidate);
                    }
                    if (step.hasOwnProperty('next') && step.next && step.next.call) {
                        chainPromise = chainPromise.then(step.next);
                    }
                    chainPromise.done(deferred.resolve)
                        .fail(deferred.reject);
                }).promise()
                    .done(function() {
                        parent.step(parent.step() + 1);
                        parent.activateStep();
                    })
                    .always(function() {
                        parent.loadingNext(false);
                    });
            }
        };
        parent.activateStep = function() {
            parent.activeStep(parent.steps()[parent.step()]);
        };
        parent.previous = function() {
            if (parent.step() > 0) {
                parent.step(parent.step() - 1);
                parent.activateStep();
            }
        };
        parent.close = function(success) {
            dialog.close(parent, {
                success: success,
                data: success ? {} : undefined
            });
            parent.closing.resolve(success);
        };
        parent.finish = function() {
            parent.running(true);
            var step = parent.steps()[parent.step()];
            step.finish()
                .done(function(data) {
                    dialog.close(parent, {
                        success: true,
                        data: data
                    });
                    parent.finishing.resolve(true);
                })
                .fail(function(data) {
                    dialog.close(parent, {
                        success: false,
                        data: data
                    });
                    parent.finishing.resolve(false);
                })
                .always(function() {
                    window.setTimeout(function() { parent.running(false); }, 500);
                });
        };
        parent.getView = function() {
            return 'views/wizards/index.html';
        };
    };
});
