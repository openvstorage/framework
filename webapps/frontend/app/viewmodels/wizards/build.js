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
            return parent.step() < parent.stepsLength() - 1 && parent.stepsLength() > 1;
        });
        parent.canNext = ko.computed(function() {
            if (parent.running()) {
                return false;
            }
            if (parent.hasNext()) {
                return parent.canContinue().value === true;
            }
            return false;
        });
        parent.hasFinish = ko.computed(function() {
            return parent.step() === parent.stepsLength() - 1;
        });
        parent.canFinish = ko.computed(function() {
            if (parent.running()) {
                return false;
            }
            if (parent.hasFinish()) {
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
                        var step = parent.steps()[parent.step()];
                        if (step.hasOwnProperty('shouldSkip') && step.shouldSkip && step.shouldSkip.call) {
                            step.shouldSkip()
                                .done(function(skip) {
                                    if (skip === true && parent.step() < parent.stepsLength() - 1) {
                                        parent.step(parent.step() + 1);
                                    }
                                    parent.activateStep();
                                });
                        } else {
                            parent.activateStep();
                        }
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
                var step = parent.steps()[parent.step()];
                if (step.hasOwnProperty('shouldSkip') && step.shouldSkip && step.shouldSkip.call) {
                    step.shouldSkip()
                        .done(function(skip) {
                            if (skip === true && parent.step() > 0) {
                                parent.step(parent.step() - 1);
                            }
                            parent.activateStep();
                        });
                } else {
                    parent.activateStep();
                }
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
            var step = parent.steps()[parent.step()],
                chainDeferred = $.Deferred(), chainPromise = chainDeferred.promise();
            $.Deferred(function(deferred) {
                chainDeferred.resolve();
                if (step.hasOwnProperty('preValidate') && step.preValidate && step.preValidate.call) {
                    chainPromise = chainPromise.then(function() {
                        return $.Deferred(function (prevalidateDeferred) {
                            step.preValidate()
                                .fail(function() {
                                    prevalidateDeferred.reject({ abort: true, data: undefined });
                                })
                                .done(prevalidateDeferred.resolve);
                        }).promise();
                    });
                }
                chainPromise.then(function() {
                        return $.Deferred(function (finishDeferred) {
                            step.finish()
                                .fail(function(data) {
                                    finishDeferred.reject({ abort: false, data: data });
                                })
                                .done(finishDeferred.resolve);
                        }).promise();
                    })
                    .done(deferred.resolve)
                    .fail(deferred.reject);
            }).promise()
                .done(function(data) {
                    dialog.close(parent, {
                        success: true,
                        data: data
                    });
                    parent.finishing.resolve(true);
                })
                .fail(function(data) {
                    if (data.abort === false) {
                        dialog.close(parent, {
                            success: false,
                            data: data.data
                        });
                        parent.finishing.resolve(false);
                    }
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
