define(['knockout'], function (ko){
    "use strict";
    var singleton = function() {
        return {
            mode: ko.observable('full')
        };
    };
    return singleton();
});