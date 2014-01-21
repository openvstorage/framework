// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define(['knockout'], function(ko){
    "use strict";
    var singleton = function() {
        return {
            messaging      : undefined,
            tasks          : undefined,
            authentication : undefined,
            defaultLanguage: 'en-US',
            language       : 'en-US',
            mode           : ko.observable('full'),
            routing        : undefined,
            footerData     : ko.observable(ko.observable()),
            nodes          : undefined
        };
    };
    return singleton();
});
