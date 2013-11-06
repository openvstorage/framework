/*global define */
define(['knockout'], function(ko){
    "use strict";
    var singleton = function() {
        return {
            messaging     : undefined,
            tasks         : undefined,
            authentication: undefined,
            mode          : ko.observable('full')
        };
    };
    return singleton();
});