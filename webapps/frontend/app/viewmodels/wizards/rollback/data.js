// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define(['knockout'], function(ko){
    "use strict";
    var singleton = function() {
        return {
            guid:     ko.observable(),
            velement: ko.observable(),
            snapshot: ko.observable()
        };
    };
    return singleton();
});
