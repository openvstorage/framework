define(['plugins/dialog', 'ovs/shared', 'knockout', '../wizards/clone/index'], function (dialog, shared, ko, CloneWizard) {
    "use strict";
    return function () {
        var self = this;

        // System
        self.shared = shared;

        // Data
        self.displayname = ko.observable('Welcome to Open vStorage');
        self.description = ko.observable('Open vStorage is the next generation storage');

        self.wizard = function () {
            return dialog.show(new CloneWizard({modal: true}));
        };
    };
});