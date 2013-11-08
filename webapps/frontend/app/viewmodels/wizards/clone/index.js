/*global define */
define([
    'jquery', 'ovs/generic',
    '../build', './gather', './confirm', './data'
], function($, generic, build, Gather, Confirm, data) {
    "use strict";
    return function(options) {
        var self = this;

        self.data = data;
        build(self);

        self.title(generic.tryGet(options, 'title', 'Clone'));
        self.modal(generic.tryGet(options, 'modal', false));

        self.data.machineGuid(options.machineguid);
        self.steps([new Gather(), new Confirm()]);

        self.activateStep();

        self.compositionComplete = function() {
            var amount = $("#amount");
            amount.on('keypress', function(e) {
                // Guard keypresses, only accepting numeric values
                return !(e.which !== 8 && e.which !== 0 && (e.which < 48 || e.which > 57));
            });
            amount.on('change', function() {
                // Guard ctrl+v
                amount.val(Math.max(1, parseInt('0' + amount.val(), 10)));
            });
        };
    };
});