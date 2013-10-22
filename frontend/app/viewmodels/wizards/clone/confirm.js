define(['knockout'], function(ko) {
    "use strict";
    return function () {
        var self = this;

        self.id = ko.observable('Confirm');
        self.name = ko.observable('Confirm entered information');
    };
});