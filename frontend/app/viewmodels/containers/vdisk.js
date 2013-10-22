define(['jquery', 'knockout', 'ovs/generic', 'ovs/authentication'], function ($, ko, generic, authentication) {
    "use strict";
    return function (data) {
        var self = this;

        // Variables
        self.load_handle = undefined;

        // Obserables
        self.guid = ko.observable(data.guid);
        self.name = ko.observable();
    };
});