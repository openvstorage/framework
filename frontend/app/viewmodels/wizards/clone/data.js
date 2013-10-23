define(['knockout'], function (ko){
    "use strict";
    var singleton = function() {
        return {
            name: ko.observable(),
            machineguid: ko.observable(),
            vm: ko.observable()
        };
    };
    return singleton();
});