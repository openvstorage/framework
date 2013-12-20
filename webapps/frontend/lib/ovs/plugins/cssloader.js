// license see http://www.openvstorage.com/licenses/opensource/
/*global define  */
define(['jquery'], function ($) {
    'use strict';
    return {
        loadCss : function (fileName) {
            var cssTag = document.createElement('link');
            cssTag.setAttribute('rel', 'stylesheet');
            cssTag.setAttribute('type', 'text/css');
            cssTag.setAttribute('href', fileName);
            cssTag.setAttribute('class', '__dynamicCss');
            document.getElementsByTagName('head')[0].appendChild(cssTag);
        },
        removeModuleCss: function () {
            $('.__dynamicCss').remove();
        }
    };
});