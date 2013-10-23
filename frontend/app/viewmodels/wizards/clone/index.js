define([
    'durandal/activator', 'plugins/dialog', 'knockout',
    'ovs/generic',
    './gather', './confirm', './data'
], function (activator, dialog, ko, generic, Gather, Confirm, data) {
    "use strict";
    return function (options) {
        var self = this;

        self.data = data;

        self.title = ko.observable();
        self.steps = ko.observableArray([]);
        self.step = ko.observable(0);
        self.activeStep = activator.create();
        self.modal = ko.observable(false);
        self.running = ko.observable(false);

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
            if (self.running()) {
                return false;
            }
            if (self.step() < self.stepsLength() - 1) {
                return self.canContinue().value;
            }
            return false;
        });
        self.hasFinish = ko.computed(function() {
            if (self.running()) {
                return false;
            }
            if (self.step() === self.stepsLength() - 1) {
                return self.canContinue().value;
            }
            return false;
        });
        self.canContinue = ko.computed(function () {
            var step = self.steps()[self.step()];
            if (step !== undefined) {
                return step.can_continue();
            }
            return {value: true, reason: undefined};
        });

        self.next = function () {
            if (self.step() < self.stepsLength() ) {
                self.step(self.step() + 1);
                self.activateStep();
            }
        };
        self.activateStep = function () {
            self.activeStep(self.steps()[self.step()]);
        };
        self.previous = function () {
            if (self.step() > 0) {
                self.step(self.step() - 1);
                self.activateStep();
            }
        };
        self.close = function (success) {
            dialog.close(self, {
                success: success,
                data: success ? {} : undefined
            });
        };
        self.finish = function() {
            self.running(true);
            var step = self.steps()[self.step()];
            step.finish()
                .done(function (data) {
                    dialog.close(self, {
                        success: true,
                        data: data
                    });
                })
                .fail(function (data) {
                    dialog.close(self, {
                        success: false,
                        data: data
                    });
                })
                .always(function () {
                    self.running(false);
                })
        };

        (function(options) {
            self.title(generic.tryget(options, 'title', 'Clone'));
            self.modal(generic.tryget(options, 'modal', false));

            self.data.machineguid(options.machineguid);
            self.steps([new Gather(), new Confirm()]);

            self.activateStep();
        }(options));
    };
});