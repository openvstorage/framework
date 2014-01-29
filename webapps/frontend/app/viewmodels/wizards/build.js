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
    'durandal/activator', 'plugins/dialog', 'knockout'
], function(activator, dialog, ko) {
    "use strict";
    return function(parent) {
        parent.title   = ko.observable();
        parent.steps   = ko.observableArray([]);
        parent.step    = ko.observable(0);
        parent.modal   = ko.observable(false);
        parent.running = ko.observable(false);

        parent.activeStep = activator.create();

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
                return parent.canContinue().value;
            }
            return false;
        });
        parent.hasFinish = ko.computed(function() {
            if (parent.running()) {
                return false;
            }
            if (parent.step() === parent.stepsLength() - 1) {
                return parent.canContinue().value;
            }
            return false;
        });
        parent.canContinue = ko.computed(function() {
            var step = parent.steps()[parent.step()];
            if (step !== undefined) {
                return step.canContinue();
            }
            return {value: true, reason: undefined};
        });

        parent.next = function() {
            if (parent.step() < parent.stepsLength() ) {
                parent.step(parent.step() + 1);
                parent.activateStep();
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
                })
                .fail(function(data) {
                    dialog.close(parent, {
                        success: false,
                        data: data
                    });
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
