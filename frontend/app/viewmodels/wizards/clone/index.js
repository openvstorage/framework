define([
    'durandal/activator', 'plugins/dialog', 'knockout',
    'ovs/generic',
    './gather', './confirm'
], function (activator, dialog, ko, generic, Gather, Confirm) {
    "use strict";
    return function (options) {
        var self = this;

        self.title = ko.observable();
        self.steps = ko.observableArray([]);
        self.step = ko.observable(0);
        self.activeStep = activator.create();
        self.modal = ko.observable(false);

        self.stepsLength = ko.computed(function() {
            return self.steps().length;
        });
        self.hasPrevious = ko.computed(function() {
            return self.step() > 0;
        });
        self.hasNext = ko.computed(function() {
            return (self.step() < self.stepsLength() - 1);
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

        (function(options) {
            self.title(generic.tryget(options, 'title', 'Clone'));
            self.modal(generic.tryget(options, 'modal', false));
            self.steps([new Gather(), new Confirm()]);
            self.activateStep();
        }(options));
    };
});