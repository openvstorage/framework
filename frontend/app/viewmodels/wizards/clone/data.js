define(['knockout'], function (ko){
    "use strict";
    var singleton = function() {
        return {
            name: ko.observable()
        };
    };
    return singleton();
});