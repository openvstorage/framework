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
/*global define, window */
define([
    'durandal/activator', 'plugins/dialog', 'knockout', 'jquery', 'ovs/generic'
], function(activator, dialog, ko, $, generic) {
    "use strict";
    /**
     * Returns a constructor which can handle stepping through a multi-view wizard
     * Wizards with multiple steps can inherit from this object to use multiple steps (inherit using .call(this)
     */
    return function() {
        var self = this;
        // Observables
        self.title       = ko.observable();
        self.step        = ko.observable(0);
        self.modal       = ko.observable(false);
        self.running     = ko.observable(false);
        self.steps       = ko.observableArray([]);
        self.loadingNext = ko.observable(false);
        self.id          = ko.observable(generic.getHash());

        // Deferreds
        self.closing   = $.Deferred();  // Track if the wizard is closing
        self.finishing = $.Deferred();  // Track if the wizard is finishing
        self.completed = $.Deferred();  // Track if the final steps finish has been completed

        // Builded variable
        self.activeStep = activator.create();

        // Computed
        self.stepsLength = ko.computed(function() {
            return self.steps().length;
        });
        self.hasPrevious = ko.computed(function() {
            if (self.running()) {
                return false;
            }
            return self.step() > 0;
        });
        self.hasNext = ko.computed(function() {
            return self.step() < self.stepsLength() - 1 && self.stepsLength() > 1;
        });
        self.canNext = ko.computed(function() {
            if (self.running()) {
                return false;
            }
            if (self.hasNext()) {
                return self.canContinue().value === true;
            }
            return false;
        });
        self.hasFinish = ko.computed(function() {
            return self.step() === self.stepsLength() - 1;
        });
        self.canFinish = ko.computed(function() {
            if (self.running()) {
                return false;
            }
            if (self.hasFinish()) {
                return self.canContinue().value === true;
            }
            return false;
        });
        self.canContinue = ko.computed(function() {
            var step = self.steps()[self.step()];
            if (step !== undefined) {
                return step.canContinue();
            }
            return { value: true, reasons: [], fields: [] };
        });

        // Functions
        /**
         * Proceed to the next step (hasNext and canNext computed will check if it is possible)
         * Before proceeding:
         *  - Calls the preValidate function (if the step has one) and waits for the deferred to resolve
         * After activating next:
         *  - Calls the shouldSkip function (if the step to transition to has one) and waits for the deferred to resolve. If true, the next step will be called
         */
        self.next = function() {
            if (self.step() < self.stepsLength() ) {
                var step = self.steps()[self.step()];
                // Build the promise chain
                var chainPromise = $.Deferred().resolve().promise();
                self.loadingNext(true);
                if (step.hasOwnProperty('preValidate') && step.preValidate && step.preValidate.call) {
                    // Return the pre-validate promise which will resolve or reject
                    chainPromise.then(function() { return step.preValidate() })
                }
                if (step.hasOwnProperty('next') && step.next && step.next.call) {
                    // Return the next promise which will resolve or reject
                    chainPromise.then(function() {
                        return step.next()
                    })
                }
                // Handle finishing of the chain
                chainPromise
                    .done(function() {
                        var next = true;
                        while (next) {
                            self.step(self.step() + 1);
                            var step = self.steps()[self.step()];
                            if (step.hasOwnProperty('shouldSkip') && step.shouldSkip && step.shouldSkip.call) {
                                step.shouldSkip()
                                    .done(function(skip) { next = skip === true && self.step() < self.stepsLength() - 1; })
                                    .fail(function() { next = false; });
                            } else {
                                next = false;
                            }
                        }
                        self.activateStep();
                    })
                    .always(function() {
                        self.loadingNext(false);
                    });
            }
        };
        /**
         * Activate a step
         * Uses the Durandal Activator to maintain the steps lifecycle
         */
        self.activateStep = function() {
            self.activeStep(self.steps()[self.step()]);
        };
        /**
         * Activates a previous steps when possible
         * After activating previous:
         *  - Calls the shouldSkip function (if the step to transition to has one) and waits for the deferred to resolve. If true, the previous step will be called
         */
        self.previous = function() {
            if (self.step() > 0) {
                var next = true;
                while (next) {
                    self.step(self.step() - 1);
                    var step = self.steps()[self.step()];
                    if (step.hasOwnProperty('shouldSkip') && step.shouldSkip && step.shouldSkip.call) {
                        step.shouldSkip()
                            .done(function (skip) {
                                next = skip === true && self.step() > 0;
                            })
                            .fail(function() {
                                next = false;
                            })
                    } else {
                        next = false;
                    }
                }
                self.activateStep();
            }
        };
        /**
         * Closes the current modal
         * @param success: Indicator to whether or not the wizard closed with success
         * @type success: bool
         */
        self.close = function(success) {
            dialog.close(self, {
                success: success,
                data: success ? {} : undefined
            });
            self.closing.resolve(success);
        };
        /**
         *  Finish and close the wizard
         *  Before proceeding:
         *  - Calls the preValidate function (if the step has one) and waits for the deferred to resolve
         *  - Calls the finish function and track the result of the finish within the completed deferred
         *  The wizard can wait for the finish to completely resolve. To enable this behaviour, the finish step should
         *  return [promise, true]. The first item is the promise, the second item is a bool whether to wait or not
         *  Not waiting for the result of the given promise means that the finish will never be unsuccessfully closed, regardless of the result
         *  The result can still be fetched from the completed property
         */
        self.finish = function() {
            self.running(true);
            var step = self.steps()[self.step()];
            // Build the promise chain, immediately resolve the deferred to kick off all the chained promises
            var chainPromise = $.Deferred().resolve().promise();
            if (step.hasOwnProperty('preValidate') && step.preValidate && step.preValidate.call) {
                chainPromise.then(function() {
                    // Return the pre-validate promise which will resolve or reject itself and mutate the data to use in our finish
                    return step.preValidate()
                        .then(function(data) {
                            return data
                        }, function(error) {
                            return {
                                abort: true,
                                data: error
                            }
                        })
                });
            }
            // Track finishing of the step into the 'completed' deferred as the wizard should close asap
            var finish_result = step.finish();
            var finish_wait = undefined;
            var finish_promise = undefined;
            // Determine what we are dealing with
            if(Array.isArray(finish_result)){
                // var [finish_promise, finish_wait] = finish_result in ES6 :(
                finish_promise = finish_result[0];
                finish_wait = finish_result[1];
            } else{
                finish_promise = finish_result;
                finish_wait = false;
            }
            // Add a handler to the promise to update our completed promise
            // Since the callbacks to a deferred respect the order in which they were added, the registers on the self.completed will fire once resolve would be called
            // This is why resolve is called with the data from the returned function
            finish_promise.then(function(data) {
                self.completed.resolve(data)
            }, function(error) {
                self.completed.reject(error)
            });
            // Add the step finish to the chain
            chainPromise.then(function() {
                if (finish_wait === true) {
                    return finish_promise.then(function(data) {
                        return data
                    }, function(error) {
                        return {
                            abort:false,
                            data: error
                        }
                    })
                }
                // No need to wait, returning empty promise to the chain
                return $.when();
            })
                // Handle finishing of the chain
                .done(function(data) {
                    dialog.close(self, {
                        success: true,
                        data: data
                    });
                    self.finishing.resolve(true);
                })
                .fail(function(data) {
                    if (data.abort === false) {
                        dialog.close(self, {
                            success: false,
                            data: data.data
                        });
                        self.finishing.resolve(false);
                    }
                })
                .always(function() {
                    window.setTimeout(function() { self.running(false); }, 500);
                });
        };
        self.getView = function() {
            return 'views/wizards/index.html';
        };
    };
});
