define(['knockout'], function(ko) {
    "use strict";
    return function () {
        var self = this;

        self.id = ko.observable('Gather');
        self.name = ko.observable('Gather information');
    };
});